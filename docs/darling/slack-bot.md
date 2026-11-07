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
- When events keep arriving for channels no route of this store owns, the service logs one warning every ten minutes naming the likely cause: another store set the app-level token on the same app.

Two stores on one store database work as well: a press changes state only while the record is pending, a message is unique by its channel and ts, and a post writes its once mark before it sends.

## What the listener does

The listener holds two Socket Mode connections. Slack sends each event to one of them, and a reconnect on one leaves the other listening. Each envelope is acknowledged the moment it arrives, before anything looks at it, so Slack never retries for a slow handler, and the payload goes on a bounded queue. One consumer then runs the filters in this order and drops what fails, counting each drop by its reason:

1. an event with a `bot_id` or the `bot_message` subtype (the alert webhook's posts and the bot's own);
2. a payload from another workspace (`team_id`, or `team.id` on a press) or another app (`api_app_id`), against the ids learned at connect;
3. an author from another workspace: a press whose `user.team_id` differs from `team.id`, or a message or mention whose `user_team` (else `team`) is not the store's workspace, as a Slack Connect member's is even though the event carries the host workspace's `team_id`; an event that names neither is dropped too;
4. a channel no route of this store names;
5. an event already seen by its type, channel and ts, one of Slack's retries.

The Web API calls and the WebSocket follow the system proxy (the HTTPS_PROXY variables or the operating system's setting), as the alert webhook and the bot's posts do; the tokens have no proxy setting of their own. Slack sends a `disconnect` about every five hours on each connection; the listener opens a new connection at once. A connection that fails is retried with a backoff that grows to a minute. Nothing is persisted: a restart loses what was queued, and Slack's own retry covers a reply sent while no connection was open. A press made while no connection was open is lost, and the card says to press again.

## Health

The service logs the listener's start and stop, each connection's open and close with Slack's reason, the first drop of each kind by the workspace and app filters, and a counters line every ten minutes.

The diagnostics bundle carries a `slack_listener` block with the same counters: connections open, the last disconnect reason and time, the drops by filter, the queue depth, the queue drops and the last event time. The block holds counts, reasons and times only, never a channel, a person or a token. It comes from `slack-listener-status.json`, which the service writes beside its log whenever the counters change and once more at stop.
