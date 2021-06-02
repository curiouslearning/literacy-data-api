class MalformedArgumentError extends Error {
  constructor(message) {
    super(message);
    this.name = "MalformedArgumentError";
  }
}
