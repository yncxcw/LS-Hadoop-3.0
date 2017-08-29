/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.nodemanager.Context;

/**
 * An implementation of the {@link ResourceUtilizationTracker} that equates
 * resource utilization with the total resource allocated to the container.
 */
public class AllocationBasedResourceUtilizationTracker implements
    ResourceUtilizationTracker {

  private static final Logger LOG =
      LoggerFactory.getLogger(AllocationBasedResourceUtilizationTracker.class);

  private ResourceUtilization containersAllocation;
  private ResourceUtilization OppContainersAllocation;
  private ResourceUtilization GuaContainersAllocation;
  
  private FixedSizeQueue recentOppProfileTime;
  private FixedSizeQueue recentOppProfilePmem;
  
  private static Clock clock = SystemClock.getInstance();
  
  //TODO estimated memory usage
  private long estimatedMemAvailable;
  private long estimatedSyncPeriod;
  private long estimatedMaxSyncPeriod;
  private long estimatedLastSyncTime;
  
  private ContainerScheduler scheduler;
  private final Context context;
  private long pMemThreshold;
  private boolean enablePmemLaunch;

  AllocationBasedResourceUtilizationTracker(ContainerScheduler scheduler,Context context) {
    this.containersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.OppContainersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.GuaContainersAllocation = ResourceUtilization.newInstance(0, 0, 0.0f);
    this.scheduler = scheduler;
    this.context=context;
    //current it is 5GB
    this.pMemThreshold= 5L<<30;
    LOG.info("threashold: "+this.pMemThreshold);
    //we set recent 20 opp container as our reference, make this parameter tunable
    this.recentOppProfileTime = new FixedSizeQueue(20);
    this.recentOppProfilePmem = new FixedSizeQueue(20);
    //initialize the estimated available
    this.estimatedMemAvailable= this.context.getNodeResourceMonitor().
            getAvailableMemory();
    
    //TODO make this configurable(ms)
    this.estimatedSyncPeriod    = 10000;
    this.estimatedMaxSyncPeriod = 10000;
    this.estimatedLastSyncTime= clock.getTime();
    
    this.enablePmemLaunch=this.context.getConf().
    		  getBoolean(YarnConfiguration.NM_ENABLE_PMEM_LAUNCH, 
    		YarnConfiguration.DEFAULT_NM_ENABLE_PMEM_LAUNCH);
  
  }

  /**
   * Get the accumulation of totally allocated resources to a container.
   * @return ResourceUtilization Resource Utilization.
   */
  @Override
  public ResourceUtilization getCurrentUtilization() {
    return this.containersAllocation;
  }

  /**
   * Add Container's resources to the accumulated Utilization.
   * @param container Container.
   */
  @Override
  public void addContainerResources(Container container) {
	LOG.info("launching new container: "+container.getContainerId()+" new resource: "
			+this.containersAllocation);
	if(container.cloneAndGetContainerStatus().getExecutionType() == ExecutionType.GUARANTEED){
		ContainersMonitor.increaseResourceUtilization(
		 getContainersMonitor(),this.GuaContainersAllocation,
		 container.getResource()
		);
	}else if(container.cloneAndGetContainerStatus().getExecutionType() == ExecutionType.OPPORTUNISTIC){
		ContainersMonitor.increaseResourceUtilization(
		  getContainersMonitor(),this.OppContainersAllocation,
		  container.getResource()
		);
	}
    ContainersMonitor.increaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  /**
   * Subtract Container's resources to the accumulated Utilization.
   * @param container Container.
   */
  @Override
  public void subtractContainerResource(Container container) {
	LOG.info("finish container: "+container.getContainerId()+" new resource: "
				+this.containersAllocation); 
	if(container.cloneAndGetContainerStatus().getExecutionType() == ExecutionType.GUARANTEED){
		ContainersMonitor.decreaseResourceUtilization(
		 getContainersMonitor(),this.GuaContainersAllocation,
		 container.getResource()
		);
	}else if(container.cloneAndGetContainerStatus().getExecutionType() == ExecutionType.OPPORTUNISTIC){
		ContainersMonitor.decreaseResourceUtilization(
		  getContainersMonitor(),this.OppContainersAllocation,
		  container.getResource()
		);
	}
	
    ContainersMonitor.decreaseResourceUtilization(
        getContainersMonitor(), this.containersAllocation,
        container.getResource());
  }

  /**
   * Check if NM has resources available currently to run the container.
   * @param container Container.
   * @return True, if NM has resources available currently to run the container.
   */
  @Override
  public boolean hasResourcesAvailable(Container container) {
	
	long pMemBytes = container.getResource().getMemorySize() * 1024 * 1024L;   
	if(enablePmemLaunch){  
		//no matter what, launch GUA containers 
		if(container.cloneAndGetContainerStatus().getExecutionType() == ExecutionType.GUARANTEED){
		
			LOG.info("has launch gua container "+container.getContainerId());
			return true;	
		}
		long averageProfile;
		if((long)recentOppProfilePmem.average() > 0){
			averageProfile = (long)recentOppProfilePmem.average();
		}else{
			averageProfile = pMemBytes;	
		}
		LOG.info(container.getContainerId()+" average "+averageProfile);
		//this is a OPP containers, use average profile
		return hasResourcesAvailable(averageProfile);
	}else{
		return hasResourcesAvailable(pMemBytes,
	         (long) (getContainersMonitor().getVmemRatio()* pMemBytes),
	        container.getResource().getVirtualCores());
		
	}
  }

  //what if we only consider realtime physical memory usage
  private boolean hasResourcesAvailable(long pMemBytes){
	  
	  LOG.info("threashold: "+this.pMemThreshold);
	   
	  if(estimatedMemAvailable < pMemBytes + pMemThreshold){
	     
		 LOG.info("estimated: "+estimatedMemAvailable+" pMem: "+pMemBytes+" no");
	     return false;         
	   }else{
		   
		 LOG.info("estimated: "+estimatedMemAvailable+" pMem: "+pMemBytes+" yes");  
		 //update estimated available pmem
		 estimatedMemAvailable-=pMemBytes;
		 return true;  
	   } 
	  
  }
  private boolean hasResourcesAvailable(long pMemBytes, long vMemBytes,
      int cpuVcores) {
	LOG.info("checking resource: "+this.containersAllocation);  
    // Check physical memory.
    if (LOG.isDebugEnabled()) {
      LOG.debug("pMemCheck [current={} + asked={} > allowed={}]",
          this.containersAllocation.getPhysicalMemory(),
          (pMemBytes >> 20),
          (getContainersMonitor().getPmemAllocatedForContainers() >> 20));
    }
    if (this.containersAllocation.getPhysicalMemory() +
        (int) (pMemBytes >> 20) >
        (int) (getContainersMonitor()
            .getPmemAllocatedForContainers() >> 20)) {
      return false;
    }

    
    if (LOG.isDebugEnabled()) {
      LOG.debug("before vMemCheck" +
              "[isEnabled={}, current={} + asked={} > allowed={}]",
          getContainersMonitor().isVmemCheckEnabled(),
          this.containersAllocation.getVirtualMemory(), (vMemBytes >> 20),
          (getContainersMonitor().getVmemAllocatedForContainers() >> 20));
    }
    // Check virtual memory.
    if (getContainersMonitor().isVmemCheckEnabled() &&
        this.containersAllocation.getVirtualMemory() +
            (int) (vMemBytes >> 20) >
            (int) (getContainersMonitor()
                .getVmemAllocatedForContainers() >> 20)) {
      return false;
    }

    float vCores = (float) cpuVcores /
        getContainersMonitor().getVCoresAllocatedForContainers();
    if (LOG.isDebugEnabled()) {
      LOG.debug("before cpuCheck [asked={} > allowed={}]",
          this.containersAllocation.getCPU(), vCores);
    }
    // Check CPU.
    if (this.containersAllocation.getCPU() + vCores > 1.0f) {
      return false;
    }
    return true;
  }

  public ContainersMonitor getContainersMonitor() {
    return this.scheduler.getContainersMonitor();
  }

 //return memory slack to instruct to kill containers
@Override
public long isCommitmentOverThreshold(long request) {
	long hostAvaiPmem=this.context.getNodeResourceMonitor().getAvailableMemory();
	LOG.info("threashold: "+this.pMemThreshold);
	long slack = hostAvaiPmem - (this.pMemThreshold + request);
	if(slack > 0){
		return slack;
	}else{
		LOG.info("memory slack "+slack+" avail "+hostAvaiPmem);
		return slack;

	}
 }

@Override
//time(millseconds) pmem(bytes)
public void addProfiledTimeAndPmem(long time, long pmem) {
	LOG.info("profileadd "+time+" "+pmem);
	this.recentOppProfilePmem.add(pmem);
	this.recentOppProfileTime.add(time);
	
}

//only called when a new container will be launched
@Override
public void syncEstimatedMemory() {
	//check  sync period in terms of ms
	long now = clock.getTime();
	if(now - estimatedLastSyncTime > estimatedSyncPeriod){
		
		//sync with actual usage
		this.estimatedMemAvailable= this.context.getNodeResourceMonitor().
				                     getAvailableMemory();
		//update sync period
		this.estimatedSyncPeriod  = Math.min((long)recentOppProfileTime.average(),this.estimatedMaxSyncPeriod);
	    //record last sync time
		this.estimatedLastSyncTime= now;
		LOG.info("profile syn: "+this.estimatedSyncPeriod);
	}
	
}


public class FixedSizeQueue{
		
	private List<Double> datas= new LinkedList<Double>();
	
	private int limit;
	
	
	public FixedSizeQueue(int limit){
		
		this.limit = limit;
	}
	
	//add element
	public synchronized void add(double e){
		
		datas.add(e);
		this.trim();
	}
	
	//trim queue by size
	public void trim(){	  
		while(datas.size() > limit){
		   datas.remove(0);	
		}
	}
	
	public double average(){
	  if(datas.size() == 0){
			  return 0.0;
	  }
	  double sum=0;
	  for(double e : datas){
		  sum += e;
	  }
	  return sum/datas.size();
	  	
	}
	
	public double max(){
	  if(datas.size() == 0){
		  return 0.0;
	  }	
	  double max=Double.MIN_VALUE;
	  for(double e : datas){
		  if(e > max)
			   max = e;
	  }
	  return max;
	}
	
	public double min(){
	  double min=Double.MAX_VALUE;
	  for(double e: datas){
		  if(e < min)
			  min = e;
	  }
	  return min;
	}	
  }


}
