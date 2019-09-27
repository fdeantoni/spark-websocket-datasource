# Spark Websocket Datasource

This is a sample project to show how to create a custom Spark Structured Streaming datasource.

To see it in action simply run the `WsSpec` test. This test includes a test websocket server using Akka HTTP. This test 
websocket server returns a stream of `WsMessage` every 1 second. 