package com.twitter.distributedlog.client.proxy;

import com.twitter.distributedlog.thrift.service.DistributedLogService;
import com.twitter.finagle.Service;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Future;
import scala.runtime.BoxedUnit;

/**
 * Cluster client
 */
public class ClusterClient {

    private final Service<ThriftClientRequest, byte[]> client;
    private final DistributedLogService.ServiceIface service;

    public ClusterClient(Service<ThriftClientRequest, byte[]> client,
                         DistributedLogService.ServiceIface service) {
        this.client = client;
        this.service = service;
    }

    public Service<ThriftClientRequest, byte[]> getClient() {
        return client;
    }

    public DistributedLogService.ServiceIface getService() {
        return service;
    }

    public Future<BoxedUnit> close() {
        return client.close();
    }
}
