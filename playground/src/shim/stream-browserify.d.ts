/** `stream-browserify` ships no types; it mirrors Node's `stream`. */
declare module "stream-browserify" {
  export * from "node:stream";
  export { default } from "node:stream";
}
