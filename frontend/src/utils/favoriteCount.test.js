import { favoriteCountAfterAction } from "./favoriteCount";

describe("favoriteCountAfterAction", () => {
  test("uses the server count from a lower-case response header", () => {
    expect(favoriteCountAfterAction({ "x-favorite-count": "17" }, 15, false)).toBe(17);
  });

  test("uses the server count from a canonical response header", () => {
    expect(favoriteCountAfterAction({ "X-Favorite-Count": "0" }, 1, true)).toBe(0);
  });

  test.each([undefined, "", "-1", "1.5", "bad"])("falls back to optimistic count for %p", (value) => {
    expect(favoriteCountAfterAction(value === undefined ? {} : { "x-favorite-count": value }, 5, false)).toBe(6);
  });

  test("does not produce a negative optimistic count", () => {
    expect(favoriteCountAfterAction({}, 0, true)).toBe(0);
  });
});
