// Canonical lifecycle ownership for the real Native/workerd tests.
// A parent PID alone is not proof after Windows reuses process IDs.
function birth(row) {
  const value = Date.parse(row?.created);
  if (!Number.isFinite(value)) throw Error("LIFECYCLE_PROCESS_IDENTITY_UNAVAILABLE");
  return value;
}

export function observedOwnedDescendants(rows, rootPid) {
  if (!Array.isArray(rows)) throw Error("LIFECYCLE_PROCESS_INVENTORY_UNAVAILABLE");
  const root = rows.find(row => row.pid === rootPid);
  if (!root) throw Error("LIFECYCLE_ROOT_IDENTITY_UNAVAILABLE");
  const ownedBirth = new Map([[rootPid, birth(root)]]);
  const result = [];
  for (let round = 0; round < 16; round++) {
    const next = rows.filter(row => {
      const parentBirth = ownedBirth.get(row.parent_pid);
      if (parentBirth === undefined || ownedBirth.has(row.pid)) return false;
      // An old OS process may retain a now-recycled parent PID.
      // Never treat it as an owned child or a valid target for taskkill.
      return birth(row) >= parentBirth;
    });
    if (!next.length) return result;
    for (const row of next) {
      ownedBirth.set(row.pid, birth(row));
      result.push(row);
    }
    if (result.length > 64) throw Error("LIFECYCLE_OWNERSHIP_LIMIT");
  }
  throw Error("LIFECYCLE_OWNERSHIP_DEPTH_LIMIT");
}
