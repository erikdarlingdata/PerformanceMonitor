# The Slack bot: setup

Darling's Slack bot posts one thread per investigation request into a route's bot channel and, with the Socket Mode listener on, hears what happens in those threads: button presses and mentions. This page is the setup: the one Slack app, the manifest, the two tokens, the per-store bot channel, and how to see that the listener is healthy.

The alert webhook is separate and unchanged. It posts alerts through an incoming webhook URL; the bot posts and listens through the app's tokens.

## One app, applied from the manifest

Use one Slack app for the alert webhooks and the bot together. The manifest is [slack-app-manifest.json](slack-app-manifest.json). It carries everything the app needs for the request threads, the listener, the buttons and the mentions, so it is applied once:

- bot token scopes `incoming-webhook`, `chat:write`, `channels:history`, `groups:history` and `app_mentions:read`;
- Socket Mode on, so the store host opens an outbound connection and needs no inbound port;
- Interactivity on, for the buttons;
- the bot events `app_mention`, `message.channels` and `message.groups`;
- incoming webhooks kept on, so the alert webhooks survive the update.

Apply it in Slack's app settings with **App Manifest** and **update from manifest** on the existing alert app. Keep `incoming_webhooks` in the manifest you apply: a manifest without it removes the app's webhooks. New scopes need a reinstall into the workspace, and a workspace administrator may have to approve them. Right after the reinstall, check every alert webhook still posts.

## The two tokens

Both tokens are saved sealed in Notifications, next to the webhook URLs, and neither is shown again once saved.

- **The bot token** (`xoxb-`, from **OAuth & Permissions** after the install). The bot posts with it, and the listener calls `auth.test` with it at connect to learn the workspace id.
- **The app-level token** (`xapp-`, scope `connections:write`). Generate it on the app's **Basic Information** page under **App-Level Tokens**, with a name for this product. The listener opens its Socket Mode connections with it. The token alone turns the listener on: saved, the service listens; blank, it does not. There is no other setting.

Save the tokens only through the setup page: the Slack bot tokens form under the routes on the web Admin page, or the desktop Settings window. Do not pass a token through an agent or an MCP call: a token sent that way sits in the calling agent's transcript. The MCP channels tool still accepts the tokens for a store with no web and no desktop, and the same warning applies to it.

Token rotation stays off. A rotating bot token starts `xoxe.xoxb-` and expires every twelve hours, and this product has no refresh flow.

## One Slack app per store for buttons and mentions

Socket Mode hands each event to one of the app's open connections. Two stores listening on one app would each acknowledge and drop the other's events, and Slack never retries an acknowledged event, so forwarding is not built. The rule:

- Each store that listens (buttons and mentions) needs a Slack app of its own, with its own app-level token.
- Posting receives no events, so several stores can share one app's bot token for the request threads. On a shared app, set the app-level token on one store only; the other stores post threads, with no buttons or mentions.
- A store still acts only on events from channels set as a bot channel on its own routes, and stays silent in threads it does not own.
- When events keep arriving for channels no route of this store owns, the service logs one line at Information every ten minutes. This is expected when other stores share the app only to post: the bot is a member of their channels, and the app subscribes to messages there. But if another store also has this app's app-level token set, the two lose each other's button presses and mentions, and each listening store needs its own Slack app.

Two stores on one store database work as well: a press changes state only while the record is pending, a message is unique by its channel and ts, and a post writes its once mark before it sends.

## What the listener does

The listener holds two Socket Mode connections. Slack sends each event to one of them, and a reconnect on one leaves the other listening. Each envelope is acknowledged the moment it arrives, before anything looks at it, so Slack never retries for a slow handler, and the payload goes on a bounded queue. One consumer then runs the filters in this order and drops what fails, counting each drop by its reason:

1. an event with a `bot_id` or the `bot_message` subtype (the alert webhook's posts and the bot's own);
2. a payload from another workspace (`team_id`, or `team.id` on a press) or another app (`api_app_id`), against the ids learned at connect;
3. an author from another workspace: a press whose `user.team_id` differs from `team.id`, or a message or mention whose `user_team` (else `team`) is not the store's workspace, as a Slack Connect member's is even though the event carries the host workspace's `team_id`; an event that names neither is dropped too;
4. a channel no route of this store names;
5. an event already seen by its type, channel and ts, one of Slack's retries.

The Web API calls and the WebSocket follow the system proxy (the HTTPS_PROXY variables or the operating system's setting), as the alert webhook and the bot's posts do; the tokens have no proxy setting of their own. Slack sends a `disconnect` about every five hours on each connection; the listener opens a new connection at once. A connection that fails is retried with a backoff that grows to a minute. Nothing is persisted: a restart loses what was queued, and Slack's own retry covers a reply sent while no connection was open. A press made while no connection was open is lost, and the card says to press again.

## Buttons

Four cards in a request's thread carry buttons. A new proposal carries Approve and Refuse. A done agent change carries Undo when its kind has an undo path: a mute, a custom rule, a threshold override, a signature merge, a plan force or unforce, and a plan guide made or dropped. A kill, an eviction and a recompile have no undo and get no button. The agent's report carries three verdict buttons, right, partly right and wrong; corrections go on the web feedback form, on the request page. A pending note written from a request carries Approve note and Reject note; a fleet note (one with no server) is decided on the web or in the Viewer only.

Approve and Undo take two presses. The first press answers the presser alone, through Slack's ephemeral reply, with the facts of a fresh dry run (a proposal) or the statement the undo would run (a change), and a Confirm button. The second press, by the same person, runs it. The grant is checked at the confirming press. Refuse, a verdict and a note decision take one press. Every refusal is ephemeral too. A press on a stored card writes a journal row with the person (or the unmapped Slack identity) and the Slack identity used, a press on a card already decided included. A press refused before its card is found leaves no journal row: one from another workspace (the listener drops and counts those before the handler sees them), one whose Confirm value cannot be read, or one on a message this store did not post; the presser is told, and the service log notes the last two. Once a card is decided, the service edits it: the buttons go, and the card says how it ended and who decided. The card's row is marked decided whether or not that edit succeeded, so when the edit failed and the buttons stayed, a later press is refused with who decided the card and when, and runs nothing. When a press fails inside the service, the presser is told that it could not be completed and that the service log has the detail; the failure's own text stays in the log.

Who may press: a person mapped on the People page to the Slack identity (the workspace id and the user id) who holds the grant for the server: `approve` with the action's kind for a proposal, `undo`, `feedback` or `decide_note`. A press by anyone else is refused, and so is a press from another workspace in a shared channel, or on a message this store did not post. A press on a proposal, change or note that is no longer pending does nothing and says so. The service acts only on cards it posted, stored by channel and ts in `config.investigation_slack_posts` (owner-written, admin reads).

## Mentions

A person asks the agent by mentioning the bot in a request's thread. Only a message that mentions the bot counts; other replies are not stored and wake nothing, so people can talk in a thread without starting an agent run. A store with no app-level token is not listening, so it gets no mentions at all; it posts threads only.

Who may ask: a person mapped on the People page to the Slack identity who holds `ask` for the request's server (the fleet, the server or one of its tags). `request_level_2` lets their request make Level 2 changes, and `request_level_3` with a list of kinds lets it propose those server actions, which always need a press. A mention from anyone else writes a journal row with no text, and the bot answers once in the thread that they have no grant. A mention outside a request thread, or on a thread whose first request opened more than 7 days ago, opens nothing, and the bot says so once.

What a mention does depends on the newest request of the thread. When that request is an unclaimed chat request, the message joins it and the person joins its askers. When it is in play (an alert request that is open or claimed, or a claimed chat request), the message waits, and the bot acknowledges the first waiting mention once; the sweep opens one chat request for every waiting message once that request has ended, applying the same rules then: a message whose asker has been disabled or has lost the grant by then, or whose thread is past 7 days, is refused as a mention would be, and goes to no agent. When it has ended, a linked chat request opens at once. A chat request copies its parent's alert rows, is stamped at the lowest of the parent's level, the level the routes give the parent's metrics and the lowest grant among its askers, is never resolved by the resolve sweep, and does not count against the route's alert limits: at most 6 chat requests open per thread per hour, and chat requests on a route stop at its `agent_max_per_hour`, counted apart from alerts. Over either cap the message waits and the bot says so. A thread holds at most 20 waiting messages and a person at most 5 of them; past that a mention is not kept and the bot says so once an hour. If no agent claims a chat request within 5 minutes, the bot says the agent may be offline, once. Every refusal (no grant, outside a request thread, an old thread, the waiting caps) is answered and journaled once per person, channel and hour, so repeating a mention gets nothing more.

The message text, with every mention of the bot removed and held to 4,000 characters, is an audit row in `config.investigation_person_messages` (owner-written, admin reads, the mcp login cannot). It reaches the agent only as `person_requests` on its claim answer, beside the brief and never inside it, with each person's name and grants, bounded to 16,000 characters of the newest messages with a count of any left out. The `ask` grant bounds what a person may ask for, not what the agent reads: an agent token reads every server, and every member of the channel reads every answer, so a route's bot channel must be a channel whose members may see data from every server. The agent answers with `post_investigation_message` while its claim is live; the message is journaled as the agent's and posted into the thread on the next pass, at the store's data detail level. Every action the agent takes under a chat request still passes the gate and the grants: a Level 3 action needs every asker to hold `request_level_3` with that kind, and a press.

## Health

The service logs the listener's start and stop, each connection's open and close with Slack's reason, the first drop of each kind by the workspace and app filters, and a counters line every ten minutes.

The diagnostics bundle carries a `slack_listener` block with the same counters: connections open, the last disconnect reason and time, the drops by filter, the queue depth, the queue drops and the last event time. The block holds counts, reasons and times only, never a channel, a person or a token. It comes from `slack-listener-status.json`, which the service writes beside its log whenever the counters change and once more at stop.
