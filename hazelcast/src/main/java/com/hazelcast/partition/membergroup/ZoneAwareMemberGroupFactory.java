/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.partition.membergroup;

import com.hazelcast.core.Member;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * ZoneAwareMemberGroupFactory is responsible for MemberGroups
 * creation according to the host metadata provided by
 * {@link DiscoveryStrategy#discoverLocalMetadata()}
 *
 * @since 3.7
 */

public class ZoneAwareMemberGroupFactory extends BackupSafeMemberGroupFactory implements MemberGroupFactory {
    DiscoveryService discoveryService;

    public ZoneAwareMemberGroupFactory(DiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
    }

    public ZoneAwareMemberGroupFactory() {
        
    }

    private void mergeEnvironmentProvidedMemberMetadata(Member localMember) {
        Map<String, Object> metadata = discoveryService.discoverLocalMetadata();
        for (Map.Entry<String, Object> entry : metadata.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Byte) {
                localMember.setByteAttribute(entry.getKey(), (Byte) value);
            } else if (value instanceof Short) {
                localMember.setShortAttribute(entry.getKey(), (Short) value);
            } else if (value instanceof Integer) {
                localMember.setIntAttribute(entry.getKey(), (Integer) value);
            } else if (value instanceof Long) {
                localMember.setLongAttribute(entry.getKey(), (Long) value);
            } else if (value instanceof Float) {
                localMember.setFloatAttribute(entry.getKey(), (Float) value);
            } else if (value instanceof Double) {
                localMember.setDoubleAttribute(entry.getKey(), (Double) value);
            } else if (value instanceof Boolean) {
                localMember.setBooleanAttribute(entry.getKey(), (Boolean) value);
            } else {
                localMember.setStringAttribute(entry.getKey(), value.toString());
            }
        }
    }

    @Override
    protected Set<MemberGroup> createInternalMemberGroups(Collection<? extends Member> allMembers) {
        Map<String, MemberGroup> groups = new HashMap<String, MemberGroup>();
        System.out.println("member attributes:");
        for (Member member : allMembers) {
            mergeEnvironmentProvidedMemberMetadata(member);
            for (String key : member.getAttributes().keySet()) {
                System.out.println(key);
                System.out.println(member.getAttributes().get(key));

            }
            final String zoneInfo = member.getStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE);
            final String rackInfo = member.getStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_RACK);
            final String hostInfo = member.getStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_HOST);
            System.out.println("zoneinfo= " + zoneInfo + " rackinfo= " + rackInfo + " hostinfo= " + hostInfo);
            if (zoneInfo == null && rackInfo == null && hostInfo == null) {
                throw new IllegalArgumentException("Not enough metadata information is provided. "
                        + "At least one of availability zone, rack or host information must be provided "
                        + "with ZONE_AWARE partition group.");
            }

            if (zoneInfo != null) {
                MemberGroup group = groups.get(zoneInfo);
                if (group == null) {
                    group = new DefaultMemberGroup();
                    groups.put(zoneInfo, group);
                }
                group.addMember(member);
            } else {
                if (rackInfo != null) {
                    MemberGroup group = groups.get(rackInfo);
                    if (group == null) {
                        group = new DefaultMemberGroup();
                        groups.put(rackInfo, group);
                    }
                    group.addMember(member);
                } else {
                    if (hostInfo != null) {
                        MemberGroup group = groups.get(hostInfo);
                        if (group == null) {
                            group = new DefaultMemberGroup();
                            groups.put(hostInfo, group);
                        }
                        group.addMember(member);
                    }
                }
            }
        }
        return new HashSet<MemberGroup>(groups.values());
    }
}
