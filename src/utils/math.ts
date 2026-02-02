export function max(...xs: [bigint, ...bigint[]]) {
  return xs.reduce((prev, x) => (x > prev ? x : prev), xs[0]);
}

export function min(...xs: [bigint, ...bigint[]]) {
  return xs.reduce((prev, x) => (x < prev ? x : prev), xs[0]);
}
