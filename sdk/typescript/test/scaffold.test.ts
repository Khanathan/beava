import { describe, it, expect } from "vitest";
import { BeavaApp } from "../src/index.js";

describe("scaffold", () => {
  it("BeavaApp constructor stores the URL", () => {
    const app = new BeavaApp("http://127.0.0.1:0");
    expect(app.url).toBe("http://127.0.0.1:0");
  });
});
