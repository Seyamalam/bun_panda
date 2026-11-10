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

export class ValueError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ValueError";
  }
}

export class KeyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "KeyError";
  }
}

export class IndexError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IndexError";
  }
}

export class ZeroDivisionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ZeroDivisionError";
  }
}
