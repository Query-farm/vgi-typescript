// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { describe, test, expect } from "bun:test";
import {
  secretType,
  secretsOfType,
  secretForScope,
  secretForScopeOfType,
  secretOfType,
  requireSecretOfType,
  secretByName,
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

// The batch is keyed by each secret's unique DuckDB NAME (vgi 4f7b75d): an
// unnamed `CREATE SECRET (TYPE vgi_example, ...)` arrives as
// `__default_vgi_example`. By-type selection must read the `type` field.
describe("by-type selection is not a key lookup", () => {
  const unnamed: SecretsDict = {
    __default_vgi_example: { type: "vgi_example", name: "__default_vgi_example", scope: "", api_key: "k0" },
  };
  const namedUnlikeType: SecretsDict = {
    conformance_creds: { type: "vgi_example", name: "conformance_creds", scope: "", api_key: "k1" },
  };

  test("an unnamed __default_<type> secret is found by type", () => {
    expect(unnamed["vgi_example"]).toBeUndefined();
    expect(secretOfType(unnamed, "vgi_example")?.api_key).toBe("k0");
    expect(secretsOfType(unnamed, "vgi_example").length).toBe(1);
    expect(requireSecretOfType(unnamed, "vgi_example").api_key).toBe("k0");
  });

  test("a secret named unlike its type is found by type", () => {
    expect(secretOfType(namedUnlikeType, "vgi_example")?.name).toBe("conformance_creds");
    expect(secretForScopeOfType(namedUnlikeType, "anything", "vgi_example")?.api_key).toBe("k1");
  });

  test("a secret NAMED like another type is not that type", () => {
    const s: SecretsDict = { vgi_example: { type: "s3", name: "vgi_example", scope: "" } };
    expect(secretOfType(s, "vgi_example")).toBeUndefined();
    expect(secretsOfType(s, "vgi_example")).toEqual([]);
    expect(secretForScopeOfType(s, "x", "vgi_example")).toBeUndefined();
    expect(secretOfType(s, "s3")?.name).toBe("vgi_example");
  });

  test("an untyped bare struct keyed by its type is the legacy fallback", () => {
    const legacy: SecretsDict = { vgi_example: { api_key: "bare" } };
    expect(secretOfType(legacy, "vgi_example")?.api_key).toBe("bare");
    expect(secretsOfType(legacy, "vgi_example").length).toBe(1);
    expect(secretForScopeOfType(legacy, "x", "vgi_example")?.api_key).toBe("bare");
    expect(secretOfType(legacy, "s3")).toBeUndefined();
  });

  test("a typed secret wins over the legacy key fallback", () => {
    const mixed: SecretsDict = {
      vgi_example: { api_key: "bare" },
      real: { type: "vgi_example", name: "real", api_key: "typed" },
    };
    expect(secretsOfType(mixed, "vgi_example").map((f) => f.api_key)).toEqual(["typed"]);
  });

  test("by type prefers an unscoped secret, else the first of the type", () => {
    const s: SecretsDict = {
      scoped: { type: "vgi_example", name: "scoped", scope: "vgi-conformance://a/" },
      plain: { type: "vgi_example", name: "plain", scope: "" },
    };
    expect(secretOfType(s, "vgi_example")?.name).toBe("plain");
    delete s.plain;
    expect(secretOfType(s, "vgi_example")?.name).toBe("scoped");
  });

  test("absent: undefined, and requireSecretOfType names the type without values", () => {
    const other: SecretsDict = { s: { type: "s3", name: "s", secret: "TOP-SECRET-VALUE" } };
    expect(secretOfType(other, "vgi_example")).toBeUndefined();
    expect(secretOfType({}, "vgi_example")).toBeUndefined();
    let msg = "";
    try {
      requireSecretOfType(other, "vgi_example");
    } catch (e) {
      msg = (e as Error).message;
    }
    expect(msg).toContain("vgi_example");
    expect(msg).not.toContain("TOP-SECRET-VALUE");
  });
});

describe("by-name selection", () => {
  const s: SecretsDict = {
    first: { type: "vgi_example", name: "first", api_key: "a" },
    __default_vgi_example: { type: "vgi_example", name: "__default_vgi_example", api_key: "d" },
    s3creds: { type: "s3", name: "s3creds" },
  };
  test("finds a secret by its name, including the default name", () => {
    expect(secretByName(s, "first", "vgi_example")?.api_key).toBe("a");
    expect(secretByName(s, "__default_vgi_example", "vgi_example")?.api_key).toBe("d");
    expect(secretByName(s, "first")?.api_key).toBe("a");
  });
  test("a missing name, or a name of another type, is undefined", () => {
    expect(secretByName(s, "nope", "vgi_example")).toBeUndefined();
    expect(secretByName(s, "s3creds", "vgi_example")).toBeUndefined();
    expect(secretByName(s, "toString", "vgi_example")).toBeUndefined();
  });
});

describe("same-type nested scopes", () => {
  const s: SecretsDict = {
    outer: { type: "vgi_example", name: "outer", scope: "vgi-conformance://a/", api_key: "outer" },
    inner: { type: "vgi_example", name: "inner", scope: "vgi-conformance://a/b/", api_key: "inner" },
    other: { type: "s3", name: "other", scope: "vgi-conformance://a/b/c/" },
  };
  test("longest prefix wins, ignoring other types", () => {
    expect(secretForScopeOfType(s, "vgi-conformance://a/b/x", "vgi_example")?.name).toBe("inner");
    expect(secretForScopeOfType(s, "vgi-conformance://a/b/c/x", "vgi_example")?.name).toBe("inner");
    expect(secretForScopeOfType(s, "vgi-conformance://a/x", "vgi_example")?.name).toBe("outer");
  });
  test("no match without an unscoped secret is undefined", () => {
    expect(secretForScopeOfType(s, "vgi-conformance://z/", "vgi_example")).toBeUndefined();
  });
  test("an unscoped secret is the fallback", () => {
    const withFallback: SecretsDict = {
      ...s,
      __default_vgi_example: { type: "vgi_example", name: "__default_vgi_example", scope: "", api_key: "fb" },
    };
    expect(secretForScopeOfType(withFallback, "vgi-conformance://z/", "vgi_example")?.name).toBe(
      "__default_vgi_example",
    );
    expect(secretForScopeOfType(withFallback, "vgi-conformance://a/b/q", "vgi_example")?.name).toBe("inner");
  });
  test("a multi-prefix scope matches on any of its prefixes", () => {
    const multi: SecretsDict = {
      m: { type: "vgi_example", name: "m", scope: "x://one/\nx://two/" },
    };
    expect(secretForScopeOfType(multi, "x://two/file", "vgi_example")?.name).toBe("m");
  });
});
