// Copyright 2026 Query Farm LLC - https://query.farm
// One immutable source for both scans and validation of published fixture facts.
export type FixtureRows = Record<string, any[]>;
function freezeRows(rows: FixtureRows): FixtureRows {
  for (const column of Object.values(rows)) Object.freeze(column);
  return Object.freeze(rows);
}
export const IMMUTABLE_CONSTRAINT_ROWS: Record<string, FixtureRows> = Object.freeze(
  Object.fromEntries(Object.entries({
  departments: {
    id: [1n, 2n, 3n],
    name: ["Engineering", "Sales", "HR"],
    budget: [500000.0, 300000.0, 200000.0],
  },
  employees: {
    id: [1n, 2n, 3n, 4n, 5n],
    name: ["Alice", "Bob", "Carol", "Dave", "Eve"],
    email: ["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"],
    department_id: [1n, 1n, 2n, 2n, 3n],
  },
  products: {
    id: [1n, 2n, 3n],
    name: ["Widget", "Gadget", "Doohickey"],
    quantity: [100n, 50n, 200n],
    price: [9.99, 24.99, 4.99],
  },
  projects: {
    department_id: [1n, 1n, 2n],
    project_code: ["P001", "P002", "P003"],
    title: ["Backend API", "Frontend UI", "Sales Portal"],
  },
  }).map(([name, rows]) => [name, freezeRows(rows)])),
);
const versions: Record<number, FixtureRows> = {
  1: { id: [1n, 2n], name: ["Alice", "Bob"] },
  2: { id: [1n, 2n, 3n], name: ["Alice", "Bob", "Carol"], email: ["a@co", "b@co", "c@co"] },
  3: {
    id: [1n, 2n, 3n], name: ["Alice", "Bob", "Carol"],
    email: ["a@co", "b@co", "c@co"], department_id: [1n, 2n, 1n],
  },
};
export const IMMUTABLE_VERSIONED_ROWS: Record<number, FixtureRows> = Object.freeze(
  Object.fromEntries(Object.entries(versions).map(([version, rows]) => [version, freezeRows(rows)])),
);
