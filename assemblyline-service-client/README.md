# Assemblyline 4 - Service Client

This Assemblyline service client is responsible for spawning service processes and facilitating the communication between the service server and service process.

This rust service client is packaged alongside the [assemblyline-v4-service](https://github.com/CybercentreCanada/assemblyline-v4-service/) package. All Assemblyline services are extended using the assemblyline-v4-service. The docker image assemblyline-v4-service-base is built using both the assemblyline-v4-package and this package.

## Configuring Service Client

Service client can be configured using environment variables.

- `RUNTIME_PREFIX`: A prefix added to files created during runtime.
- `TASKING_DIR`: A directory where all task related files are write to.
- `SERVICE_API_HOST`: The hostname of the service server.
- `SERVICE_API_KEY`: API key for communication with the service server.
- `HOSTNAME`: Name of the container that this service is running on.
- `SERVICE_SERVER_ROOT_CA_PATH`: Path to Root CA for TLS connection to the service server.
- `CONTAINER_MODE`: In container mode, the process will terminate when errors are caught. Otherwise, the process will restart itself after error.
- `REGISTER_ONLY`: When true, the service process will only register the service manifest with the service server and terminate afterwards.
- `SERVICE_DIR`: The directory where the service module is located.
- `MANIFEST_DIR`: The directory containing `service_manifest.yml` of the service.
- `AL_SERVICE_TASK_LIMIT`: Limit the number of tasks that this service processes. The service client will terminate when the limit is reached.
- `ASSEMBLYLINE_CONFIG_PATH`: Path to the Assemblyline configuration file.


## Service Client's Communication with the Service Process

The service client uses two FIFO pipes to communicate with service processes. There are two FIFO pipes, `task.fifo` and `done.fifo`.


When a task is received, the service client write the task file to a tasking directory for that specific task, and the service client writes the message `[{tasking_directory, tasking_file_path}]`. The service process is listening to this `task.fifo`, the the message is received, the service process will begin working on the task.

When the service process finished processing a task, the service process writes a message `[{TASK_DONE_FILE_PATH, TASK_DONE_STATUS]` to `done.fifo`. Service client reads the message from `done.fifo` and process the result of the task and upload all relevant task files and data to the service server.
