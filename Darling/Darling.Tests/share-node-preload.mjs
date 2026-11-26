/* #9789: registers share-node-hooks.mjs so Node can import the shipped web modules when the suite runs from a network share.
   ShareNodeLoader.cs puts this file in NODE_OPTIONS (as --import) for every Node process a test starts, and only when the repository is on a share. */
import { register } from "node:module";

register("./share-node-hooks.mjs", import.meta.url);
