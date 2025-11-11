export function quoteIdentifier(identifier: string): string {
  return identifier
    .split(".")
    .map((segment) => `"${segment.replace(/"/g, '""')}"`)
    .join(".");
}

export function sanitizeChannelName(name: string): string {
  return name.replace(/[^a-zA-Z0-9_]/g, "_");
}

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function toNumber(value: string | number | null | undefined): number {
  if (typeof value === "number") {
    return value;
  }
  if (value === null || value === undefined) {
    return 0;
  }
  const asNumber = Number(value);
  if (Number.isNaN(asNumber)) {
    throw new Error(`Expected numeric value, received ${value}`);
  }
  return asNumber;
}
