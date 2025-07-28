function log(level, message) {
  const logObject = {
    level,
    message,
    timestamp: new Date().toISOString(),
    job: "cdc-app",
    source: "stdout"
  };
  console.log(JSON.stringify(logObject));
}

setInterval(() => {
  log("debug", "simples debug");
  log("info", "mensagem informativa");
  log("warn", "aviso importante");
  log("error", "erro crítico");
}, 5000);