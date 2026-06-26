// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { describe, test, expect } from "bun:test";
import {
  secretType,
  secretsOfType,
  secretForScope,
  secretForScopeOfType,
  type SecretsDict,
} from "../helpers.js";

const secrets: SecretsDict = {
  my_s3: { type: "s3", key_id: "AAA", scope: "s3://bucket-a" },
  my_s3_b: { type: "s3", key_id: "BBB", scope: "s3://bucket-b\ns3://bucket-b2" },
  my_gcs: { type: "gcs", key_id: "G" },
};

describe("resolved-secret type/scope selection", () => {
  test("type-aware accessors", () => {
    expect(secretType(secrets, "my_s3")).toBe("s3");
    expect(secretType(secrets, "my_gcs")).toBe("gcs");
    expect(secretsOfType(secrets, "s3").length).toBe(2);
    expect(secretsOfType(secrets, "gcs").length).toBe(1);
    expect(secretsOfType(secrets, "azure").length).toBe(0);
  });

  test("scope+type selection per bucket", () => {
    expect(secretForScopeOfType(secrets, "s3://bucket-a/x.dat", "s3")?.key_id).toBe("AAA");
    expect(secretForScopeOfType(secrets, "s3://bucket-b2/y.dat", "s3")?.key_id).toBe("BBB");
  });

  test("longest prefix wins; unscoped is fallback", () => {
    const s: SecretsDict = {
      broad: { type: "s3", key_id: "broad", scope: "s3://bucket" },
      narrow: { type: "s3", key_id: "narrow", scope: "s3://bucket/data" },
    };
    expect(secretForScope(s, "s3://bucket/data/x.dat")?.key_id).toBe("narrow");
    expect(secretForScope(s, "s3://bucket/other/x.dat")?.key_id).toBe("broad");

    const unscoped: SecretsDict = { only: { type: "s3", key_id: "only" } };
    expect(secretForScope(unscoped, "s3://any/x")?.key_id).toBe("only");

    expect(secretForScope(s, "s3://nope/x")).toBeUndefined();
  });
});
