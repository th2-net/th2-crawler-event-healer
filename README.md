# Healer data processor (0.0.2)

Healer data processor fixes wrong status of events. When a child event has a "failed" status and its parent
has a "success" status, the status of the parent is wrong. Healer finds the parent event and makes its status "failed", too.

## Configuration

There is an example of full configuration (infra-2.0) for the data processor

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: event-healer
spec:
  image-name: ghcr.io/th2-net/th2-crawler-event-healer
  image-version: <verison>
  type: th2-conn
  custom-config:
    stateSessionAlias: my-processor-state
    enableStoreState: false
    from: 2021-06-16T12:00:00.00Z
    to: 2021-06-17T14:00:00.00Z
    intervalLength: PT10M
    syncInterval: PT10M
    awaitTimeout: 10
    awaitUnit: SECONDS
    events:
      bookToScope:
        book1: []
        book2: []
    processorSettings:
      name: test-event-healer
      version: 1.0.0
      maxCacheCapacity: 1000
  pins:
    grpc:
      client:
        - name: to_data_provider
          service-class: com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
        - name: to_data_provider_stream
          service-class: com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
          linkTo:
            - box: lw-data-provider
              pin: server
  extended-settings:
    service:
      enabled: true
      type: ClusterIP
      endpoints:
        - name: 'grpc'
          targetPort: 8080
    envVariables:
      JAVA_TOOL_OPTIONS: '-XX:+ExitOnOutOfMemoryError -XX:+UseContainerSupport -XX:MaxRAMPercentage=85'
  resources:
    limits:
      memory: 200Mi
      cpu: 200m
    requests:
      memory: 100Mi
      cpu: 50m
```

### Parameters description

+ name - the data processor name
+ version - the data processor version
+ maxCacheCapacity - the maximum capacity of the cache that stores 
  events processed by Healer. Caching events is useful in order to 
  avoid their repeated retrieval from Cradle.
  After reaching the maximum capacity, the least recent accessed event 
  from the cache will be removed, so no overflow occurs.

See th2-crawler in **Useful links** section to see the goal of *name* and *version* 
parameters.

# Useful links

+ th2-common - https://github.com/th2-net/th2-common-j

+ th2-crawler - https://github.com/th2-net/th2-crawler