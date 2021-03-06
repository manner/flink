package org.apache.flink.connector.hbase.source.hbaseendpoint;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

/**
 * Reduces {@link AdminProtos.AdminService.BlockingInterface} to interface necessary for WAL
 * replication ({@link AdminProtos.AdminService.BlockingInterface#replicateWALEntry(RpcController,
 * AdminProtos.ReplicateWALEntryRequest)}) to keep implementing class small.
 */
public interface ReplicationTargetInterface extends AdminProtos.AdminService.BlockingInterface {

    @Override
    public default AdminProtos.ClearCompactionQueuesResponse clearCompactionQueues(
            RpcController arg0, AdminProtos.ClearCompactionQueuesRequest arg1)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"clearCompactionQueues\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.GetRegionInfoResponse getRegionInfo(
            RpcController controller, AdminProtos.GetRegionInfoRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getRegionInfo\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.GetStoreFileResponse getStoreFile(
            RpcController controller, AdminProtos.GetStoreFileRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getStoreFile\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.GetOnlineRegionResponse getOnlineRegion(
            RpcController controller, AdminProtos.GetOnlineRegionRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getOnlineRegion\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.OpenRegionResponse openRegion(
            RpcController controller, AdminProtos.OpenRegionRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"openRegion\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.WarmupRegionResponse warmupRegion(
            RpcController controller, AdminProtos.WarmupRegionRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"warmupRegion\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.CloseRegionResponse closeRegion(
            RpcController controller, AdminProtos.CloseRegionRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"closeRegion\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.FlushRegionResponse flushRegion(
            RpcController controller, AdminProtos.FlushRegionRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"flushRegion\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.CompactionSwitchResponse compactionSwitch(
            RpcController controller, AdminProtos.CompactionSwitchRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"compactionSwitch\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.CompactRegionResponse compactRegion(
            RpcController controller, AdminProtos.CompactRegionRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"compactRegion\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.ReplicateWALEntryResponse replay(
            RpcController controller, AdminProtos.ReplicateWALEntryRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"replay\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.RollWALWriterResponse rollWALWriter(
            RpcController controller, AdminProtos.RollWALWriterRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"rollWALWriter\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.GetServerInfoResponse getServerInfo(
            RpcController controller, AdminProtos.GetServerInfoRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getServerInfo\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.StopServerResponse stopServer(
            RpcController controller, AdminProtos.StopServerRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"stopServer\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(
            RpcController controller, AdminProtos.UpdateFavoredNodesRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"updateFavoredNodes\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.UpdateConfigurationResponse updateConfiguration(
            RpcController controller, AdminProtos.UpdateConfigurationRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"updateConfiguration\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.GetRegionLoadResponse getRegionLoad(
            RpcController controller, AdminProtos.GetRegionLoadRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getRegionLoad\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.ClearRegionBlockCacheResponse clearRegionBlockCache(
            RpcController controller, AdminProtos.ClearRegionBlockCacheRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"clearRegionBlockCache\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.ExecuteProceduresResponse executeProcedures(
            RpcController controller, AdminProtos.ExecuteProceduresRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"executeProcedures\" not implemented in class " + getClass());
    }

    @Override
    public default QuotaProtos.GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(
            RpcController arg0, QuotaProtos.GetSpaceQuotaSnapshotsRequest arg1)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getSpaceQuotaSnapshots\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.SlowLogResponses getSlowLogResponses(
            RpcController controller, AdminProtos.SlowLogResponseRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getSlowLogResponses\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.SlowLogResponses getLargeLogResponses(
            RpcController controller, AdminProtos.SlowLogResponseRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"getLargeLogResponses\" not implemented in class " + getClass());
    }

    @Override
    public default AdminProtos.ClearSlowLogResponses clearSlowLogsResponses(
            RpcController controller, AdminProtos.ClearSlowLogResponseRequest request)
            throws ServiceException {
        throw new UnsupportedOperationException(
                "Operation \"clearSlowLogsResponses\" not implemented in class " + getClass());
    }
}
