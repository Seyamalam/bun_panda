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

/** pandas `pandas.errors.UndefinedVariableError` — raised by query()/eval() for unknown names. */
export class UndefinedVariableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UndefinedVariableError";
  }
}

/** Python `NotImplementedError` — raised for query()/eval() syntax pandas does not support. */
export class NotImplementedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NotImplementedError";
  }
}

/** Python `AttributeError` — pandas raises it for operators its expression parser lacks. */
export class AttributeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AttributeError";
  }
}
