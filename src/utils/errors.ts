export function tryCatch<T>(fn: () => T) {
  try {
    return { result: fn() };
  } catch (error) {
    return { error };
  }
}
