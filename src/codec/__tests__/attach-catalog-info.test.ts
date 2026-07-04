import { describe, test, expect } from "bun:test";
import { encodeASD, decodeASD } from "../asd.js";
import { AttachCatalogInfoSchema } from "../../generated/vgi-protocol-schemas.js";

describe("AttachCatalogInfo", () => {
  test("round-trip with options map + booleans", () => {
    const v = {
      alias: "acme_lake",
      target: "ducklake:sqlite:/data/meta.sqlite",
      db_type: "",
      options: { DATA_PATH: "/data/" },
      hidden: true,
      required: true,
      secret_ref: "pg",
    };
    const bytes = encodeASD(AttachCatalogInfoSchema, v);
    const back = decodeASD<typeof v>(AttachCatalogInfoSchema, bytes);
    expect(back.alias).toBe("acme_lake");
    expect(back.target).toBe("ducklake:sqlite:/data/meta.sqlite");
    expect(back.options).toEqual({ DATA_PATH: "/data/" });
    expect(back.hidden).toBe(true);
    expect(back.required).toBe(true);
    expect(back.secret_ref).toBe("pg");
  });
});
