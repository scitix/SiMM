#include <gtest/gtest.h>

#include "common/errcode/errcode_def.h"

TEST(ErrcodeDefTest, CommonErrorCodes) {
    EXPECT_EQ(CommonErr::OK, 0);
    EXPECT_EQ(CommonErr::InvalidArgument, -1000);
    EXPECT_EQ(CommonErr::NotImplemented, -1001);
    EXPECT_EQ(CommonErr::UnknownError, -2000);
}

TEST(ErrcodeDefTest, ClientErrorCodes) {
    EXPECT_EQ(ClntErr::GetRoutingTableTimeout, -2000);
    EXPECT_EQ(ClntErr::GetRoutingTableFailed, -2001);
}

TEST(ErrcodeDefTest, ClusterManagerErrorCodes) {
    EXPECT_EQ(CmErr::InitFailed, -3000);
    EXPECT_EQ(CmErr::ClusterManagerAlreadyStarted, -3001);
    EXPECT_EQ(CmErr::ClusterManagerAlreadyStopped, -3002);
    EXPECT_EQ(CmErr::InitIntraRPCServiceFailed, -3003);
    EXPECT_EQ(CmErr::InitInterRPCServiceFailed, -3004);
    EXPECT_EQ(CmErr::InitAdminRPCServiceFailed, -3005);
    EXPECT_EQ(CmErr::InitShardManagerFailed, -3006);
    EXPECT_EQ(CmErr::InitNodeManagerFailed, -3007);
    EXPECT_EQ(CmErr::InitShardPlacementSchedulerFailed, -3008);
    EXPECT_EQ(CmErr::InitHBMonitorFailed, -3009);
    EXPECT_EQ(CmErr::InitResourceMonitorFailed, -3010);
    EXPECT_EQ(CmErr::InitDataSyncerFailed, -3011);
    EXPECT_EQ(CmErr::InitDataserverNodesTooFew, -3100);
    EXPECT_EQ(CmErr::InitDataserverNodesTooMany, -3101);
    EXPECT_EQ(CmErr::InvalidShardId, -3200);
}

TEST(ErrcodeDefTest, DataServerErrorCodes) {
    EXPECT_EQ(DsErr::InitFailed, -4000);
}

TEST(ErrcodeDefTest, ErrString) {
    EXPECT_EQ(ERR_STR(CommonErr::OK), "CommonErr::OK");
    EXPECT_EQ(ERR_STR(CommonErr::InvalidArgument), "CommonErr::InvalidArgument");
    EXPECT_EQ(ERR_STR(CommonErr::NotImplemented), "CommonErr::NotImplemented");
    EXPECT_EQ(ERR_STR(CommonErr::UnknownError), "CommonErr::UnknownError");

    EXPECT_EQ(ERR_STR(ClntErr::GetRoutingTableTimeout), "ClntErr::GetRoutingTableTimeout");
    EXPECT_EQ(ERR_STR(ClntErr::GetRoutingTableFailed), "ClntErr::GetRoutingTableFailed");

    EXPECT_EQ(ERR_STR(CmErr::InitFailed), "CmErr::InitFailed");
    EXPECT_EQ(ERR_STR(CmErr::ClusterManagerAlreadyStarted), "CmErr::ClusterManagerAlreadyStarted");
    EXPECT_EQ(ERR_STR(CmErr::ClusterManagerAlreadyStopped), "CmErr::ClusterManagerAlreadyStopped");
    EXPECT_EQ(ERR_STR(CmErr::InitIntraRPCServiceFailed), "CmErr::InitIntraRPCServiceFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitInterRPCServiceFailed), "CmErr::InitInterRPCServiceFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitAdminRPCServiceFailed), "CmErr::InitAdminRPCServiceFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitShardManagerFailed), "CmErr::InitShardManagerFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitNodeManagerFailed), "CmErr::InitNodeManagerFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitShardPlacementSchedulerFailed), "CmErr::InitShardPlacementSchedulerFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitHBMonitorFailed), "CmErr::InitHBMonitorFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitResourceMonitorFailed), "CmErr::InitResourceMonitorFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitDataSyncerFailed), "CmErr::InitDataSyncerFailed");
    EXPECT_EQ(ERR_STR(CmErr::InitDataserverNodesTooFew), "CmErr::InitDataserverNodesTooFew");
    EXPECT_EQ(ERR_STR(CmErr::InitDataserverNodesTooMany), "CmErr::InitDataserverNodesTooMany");

    EXPECT_EQ(ERR_STR(DsErr::InitMempoolFailed), "DsErr::InitMempoolFailed");
}
