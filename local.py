from rpc import app, config

print("RUNNING SERVER WITH CONFIG:")
print(config.to_json())

app.run(host=config.serve_host, port=config.web_server_port, threaded=True)
