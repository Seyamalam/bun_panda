export class BunPandaValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BunPandaValidationError";
  }
}

export class NotSupportedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NotSupportedError";
  }
}
