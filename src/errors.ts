export class BunPandaValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BunPandaValidationError";
  }
}
