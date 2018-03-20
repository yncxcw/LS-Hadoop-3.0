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

package org.apache.hadoop.yarn.sls.nodemanager;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords
        .RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords
        .RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;

@Private
@Unstable
public class NMSimulator extends TaskRunner.Task {
  // node resource
  private RMNode node;
  // master key
  private MasterKey masterKey;
  // containers with various STATE
  private List<ContainerId> completedContainerList;
  private List<ContainerId> releasedContainerList;
  private List<ContainerId> killedContainerList;
  private DelayQueue<ContainerSimulator> containerQueue;
  private Map<ContainerId, ContainerSimulator> runningContainers;
  private List<ContainerId> amContainerList;
  //opportu,queue would be the sub of the below two
  private Map<Long, ContainerSimulator> oppContainerQueuing;
  private List<ContainerId> oppContainerRunning;
  
 
  //to bookkeeping current used physical memory and virtual memory
  //we define the virtual memory as the user requested memory
  private ResourceUtilization nodeUtilization;
  //opportunistic containers support
  private OpportunisticContainersStatus opportunisticContainersStatus;
  // resource manager
  private ResourceManager rm;
  // heart beat response id
  private int RESPONSE_ID = 1;
  //checked by virtual memory of physical memory
  private boolean byVirtual=false;
  //queuing limit for nodemanager
  private int queuingLimit;
  
  private final static Logger LOG = Logger.getLogger(NMSimulator.class);
  
  public void init(String nodeIdStr, int memory, int cores,
          int dispatchTime, int heartBeatInterval, ResourceManager rm, boolean byVirtual, int queuingLimit)
          throws IOException, YarnException {
    super.init(dispatchTime, dispatchTime + 1000000L * heartBeatInterval,
            heartBeatInterval);
    // create resource
    String rackHostName[] = SLSUtils.getRackHostName(nodeIdStr);
    this.node = NodeInfo.newNodeInfo(rackHostName[0], rackHostName[1], 
                  BuilderUtils.newResource(memory, cores));
    this.byVirtual=byVirtual;
    
    this.queuingLimit=queuingLimit;
    
    this.rm = rm;
    // init data structures
    completedContainerList=
            Collections.synchronizedList(new ArrayList<ContainerId>());
    // killed by nm container list
    killedContainerList=
    		Collections.synchronizedList(new ArrayList<ContainerId>());
    //running opp containers list
    oppContainerRunning=
    		Collections.synchronizedList(new ArrayList<ContainerId>());
    //opp container queuing and running list
    oppContainerQueuing=new TreeMap<Long, ContainerSimulator>();
    releasedContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    containerQueue = new DelayQueue<ContainerSimulator>();
    amContainerList =
            Collections.synchronizedList(new ArrayList<ContainerId>());
    runningContainers =
            new ConcurrentHashMap<ContainerId, ContainerSimulator>();
    // register NM with RM
    RegisterNodeManagerRequest req =
            Records.newRecord(RegisterNodeManagerRequest.class);
    req.setNodeId(node.getNodeID());
    req.setResource(node.getTotalCapability());
    req.setHttpPort(80);
    RegisterNodeManagerResponse response = rm.getResourceTrackerService()
            .registerNodeManager(req);
    masterKey = response.getNMTokenMasterKey();
    
    nodeUtilization=ResourceUtilization.newInstance(0, 0, 0);
    //initialized opp status
    opportunisticContainersStatus=OpportunisticContainersStatus.newInstance();
    opportunisticContainersStatus.setEstimatedQueueWaitTime(0);
    opportunisticContainersStatus.setOpportCoresUsed(0);
    opportunisticContainersStatus.setOpportMemoryUsed(0);
    opportunisticContainersStatus.setQueuedOpportContainers(0);
    opportunisticContainersStatus.setRunningOpportContainers(0);
    opportunisticContainersStatus.setWaitQueueLength(0);
  }

  @Override
  public void firstStep() {
    // do nothing
  }
  
  public void updateOppStatistics(){
	  //update oppor status;
	  int oppWaitTime=0;
	  int oppCores=0;
	  int oppMems=0;
	  int oppQueues=0;
	  int oppRunnings=0;
	  int oppQueueopps=0;
	  synchronized(oppContainerRunning){
	   	  
	   /*
	    * We use node allocated memory and core to set opp memory and cores	  
	   for(ContainerId cntId:oppContainerRunning){ 	
	     oppCores+=runningContainers.get(cntId).getResource().getVirtualCores();
	     oppMems+=runningContainers.get(cntId).getResource().getMemorySize();
	     oppRunnings+=1;
	   }
	   */
	  
	    for(ContainerSimulator container:runningContainers.values()){
	    	
	      oppMems+=container.getResource().getMemorySize();
	      oppCores+=container.getResource().getVirtualCores();
	      // LOG.info("contaienr "+container.getId()+" memory: "+containerMemory);	 
	    }
	  }
	  oppQueueopps=oppContainerQueuing.size();
	  //TODO theoritical the oppQueues = queued guaranteed + queued opp, we do a simplicity here.
	  oppQueues= oppQueueopps;
	  //TODO how to estimate this value
	  this.opportunisticContainersStatus.setEstimatedQueueWaitTime(0);
	  //we use node used core instead
	  this.opportunisticContainersStatus.setOpportCoresUsed(oppCores);
	  //we use node used memory instead
	  this.opportunisticContainersStatus.setOpportMemoryUsed(oppMems);
	  this.opportunisticContainersStatus.setQueuedOpportContainers(oppQueueopps);
	  this.opportunisticContainersStatus.setRunningOpportContainers(oppRunnings);
	  this.opportunisticContainersStatus.setWaitQueueLength(oppQueues);
	  
  }
  
  //TODO check if there are enough resource for new launched container
  /*
   * if byvirtual is true, checked is performed by suing virtual memory<requested at launch>
   * otherwise, check is performed by using physical memory<update periodically>
   */
  public long checkMemoryAvailable(ContainerSimulator container){
	  
	  long usedMemory    = byVirtual ? nodeUtilization.getVirtualMemory():nodeUtilization.getPhysicalMemory();
	  long containerMemory=0;
	  if(container!=null)
	     containerMemory = byVirtual ? container.getResource().getMemorySize() : container.pullCurrentMemoryUsuage(System.currentTimeMillis()); 
	 //LOG.info("node "+node.getHostName()+" current used "+usedMemory+" new request "+containerMemory); 
	 return node.getTotalCapability().getMemorySize() - usedMemory - containerMemory;
  }
  
  
  public void killOppRuningContainers(List<ContainerSimulator> toKills){
	  for(ContainerSimulator container : toKills){
		  ContainerId containerId=container.getId();
		  ContainerSimulator cs = runningContainers.remove(containerId);
		  containerQueue.remove(cs);
		  oppContainerRunning.remove(cs.getId());
		  synchronized(killedContainerList){
		     killedContainerList.add(containerId);
		  }
		 //updating node resources
	      int physicalMemory=(int)cs.pullCurrentMemoryUsuage(System.currentTimeMillis());
	      int virtualMemory =(int)cs.getResource().getMemorySize();
	      nodeUtilization.setPhysicalMemory((int)nodeUtilization.getPhysicalMemory()-physicalMemory);
	      nodeUtilization.setVirtualMemory((int)nodeUtilization.getVirtualMemory()-virtualMemory);
	     
	      LOG.info("killing container "+container.getId()+" on node "+node.getNodeID()+
				  " phy utilization "+nodeUtilization.getPhysicalMemory()+
				  " vir utilization "+nodeUtilization.getVirtualMemory());
	      
	  }
  }

  //launch a container, updating node resoruces
  public void luanchContainer(ContainerSimulator container){
	  //update the endtime before running
	  long currentTimeMillis=System.currentTimeMillis();
	  container.setEndTime(currentTimeMillis+container.getLifeTime());
	   
	  containerQueue.add(container);
	  if(container.getExeType().getExecutionType().equals(ExecutionType.OPPORTUNISTIC)){
		oppContainerRunning.add(container.getId());  
	  }
	  runningContainers.put(container.getId(), container);
	  int physicalMemory=(int)container.pullCurrentMemoryUsuage(System.currentTimeMillis());
      int virtualMemory =(int)container.getResource().getMemorySize();
      nodeUtilization.setPhysicalMemory((int)nodeUtilization.getPhysicalMemory()+physicalMemory);
      nodeUtilization.setVirtualMemory((int)nodeUtilization.getVirtualMemory()+virtualMemory);
      
      LOG.info("launching container "+container.getId()+" on node "+node.getNodeID()+
			  " phy utilization "+nodeUtilization.getPhysicalMemory()+
			  " vir utilization "+nodeUtilization.getVirtualMemory());
    
  }
  //finish  containers by polling @containerQueue, updating node resource
  public void finishContainer(){
	// we check the lifetime for each running containers
	    ContainerSimulator cs = null;
	    synchronized(completedContainerList) {
	      while ((cs = containerQueue.poll()) != null) {
	        runningContainers.remove(cs.getId());
	        //remove opp containers
	        oppContainerRunning.remove(cs.getId());
	        completedContainerList.add(cs.getId());
	        //updating node resources
	        int physicalMemory=(int)cs.pullCurrentMemoryUsuage(System.currentTimeMillis());
	        int virtualMemory =(int)cs.getResource().getMemorySize();
	        nodeUtilization.setPhysicalMemory((int)nodeUtilization.getPhysicalMemory()-physicalMemory);
	        nodeUtilization.setVirtualMemory((int)nodeUtilization.getVirtualMemory()-virtualMemory);
	        LOG.debug(MessageFormat.format("Container {0} has completed",
	                cs.getId()));
	      }
	    }
	   //after some containers are finished, there could be some opportunities to launch opp containers
	   launchQueuedOppContainers(); 
  }
  
  //remove a running container due to preemption or failure, updating node resources
  public void removeContainer(ContainerSimulator container){
	  ContainerId containerId=container.getId();
	  ContainerSimulator cs = runningContainers.remove(containerId);
      containerQueue.remove(cs);
      oppContainerRunning.remove(cs.getId());
      //it has been procted by outer lock
      releasedContainerList.add(containerId);
      //updating node resources
      int physicalMemory=(int)cs.pullCurrentMemoryUsuage(System.currentTimeMillis());
      int virtualMemory =(int)cs.getResource().getMemorySize();
      nodeUtilization.setPhysicalMemory((int)nodeUtilization.getPhysicalMemory()-physicalMemory);
      nodeUtilization.setVirtualMemory((int)nodeUtilization.getVirtualMemory()-virtualMemory);
      LOG.debug(MessageFormat.format("NodeManager {0} releases a " +
          "container ({1}).", node.getNodeID(), containerId));
	  
  }
  
  public void launchQueuedOppContainers(){
	  //the @oppContainerQueuing is ordered by their submission time
	  synchronized(oppContainerQueuing){
	  Iterator<Entry<Long, ContainerSimulator>> entryIt = oppContainerQueuing.entrySet().iterator();
	  while(entryIt.hasNext()){
		  Entry<Long, ContainerSimulator> entry = entryIt.next();
		  ContainerSimulator cs=entry.getValue();
		 if(checkMemoryAvailable(cs) > 0){
			 LOG.info("launch opp container "+cs.getId());
			 luanchContainer(cs);
	    	 //remove from the queue.
	    	 entryIt.remove();
	         //stop launching more opp containers.	 
	      }else{
	    	break;
	      } 
	  }
	 }
  }
  /**
   * try to kill numbers of containers to free resource of memory  
   * resource, the amount of resource needed to freed 
   */
  public int killContainersToFreeMemory(long demandMemory){
	 LOG.info("try to kill to free "+demandMemory); 
	 //start searching from the beginning of the opp running list
	 int  index=oppContainerRunning.size()-1; 
	 List<ContainerSimulator> toKills=new ArrayList<ContainerSimulator>();
 	 while(demandMemory > 0 && index >= 0){
 		 //always kill newly launched container
		 ContainerId cntId=oppContainerRunning.get(index);
		 ContainerSimulator container=runningContainers.get(cntId);
		 //oppContainerRunning.
		 long cntMemory;
		 if(byVirtual)
			 cntMemory = container.getResource().getMemorySize();
		 else
			 cntMemory = container.pullCurrentMemoryUsuage(System.currentTimeMillis());
		 demandMemory-=cntMemory;
		 index--;
		 toKills.add(container);
		 
	 }
 	killOppRuningContainers(toKills); 
	return toKills.size();  
  }
  

  @Override
  public void middleStep() throws Exception {
	//poll if we find containers to finish
	finishContainer();
    //update node memory usage
    long nodeUsedMemory=0;
    for(ContainerSimulator container:runningContainers.values()){
    	  long containerMemory=container.pullCurrentMemoryUsuage(System.currentTimeMillis());
    	  nodeUsedMemory+=containerMemory;
    	 // LOG.info("contaienr "+container.getId()+" memory: "+containerMemory);	 
    }
    //TODO add set virtual memory support
    //LOG.info("node: "+node.getHostName()+" newly pmem "+nodeUsedMemory);
    nodeUtilization.setPhysicalMemory((int)nodeUsedMemory);
    
    //only works for physical memory
    if(!byVirtual){
    //kill overcommit memory during runtime
     long overcommitMemory = checkMemoryAvailable(null);
     if(overcommitMemory < 0){
    	killContainersToFreeMemory(-overcommitMemory);
     }
     //launch queued opp containers
     if(overcommitMemory > 0){
    	launchQueuedOppContainers();
     }
    }
    
    //update opp statistics
    updateOppStatistics();
    
    //code to test kill function
    /*
    if(runningContainers.size() >= 1){
    	double randNum=Math.random() * ( 100 - 0 );
    	if(randNum > 5){
    		LOG.info("find container to kill");
    		Entry<ContainerId,ContainerSimulator>test_container=runningContainers.entrySet().iterator().next();
    		List<ContainerSimulator> toKills=new ArrayList<ContainerSimulator>();
    	    toKills.add(test_container.getValue());
    	    killOppRuningContainers(toKills); 
    	}
    }
    */
    // send heart beat
   
    NodeHeartbeatRequest beatRequest =
            Records.newRecord(NodeHeartbeatRequest.class);
    beatRequest.setLastKnownNMTokenMasterKey(masterKey);
    NodeStatus ns = Records.newRecord(NodeStatus.class);
    
    //Set node utilization, we only consider the memory utilization
    ns.setNodeUtilization(nodeUtilization);
    ns.setContainersUtilization(nodeUtilization);
    
    ns.setContainersStatuses(generateContainerStatusList());
    ns.setNodeId(node.getNodeID());
    ns.setKeepAliveApplications(new ArrayList<ApplicationId>());
    ns.setResponseId(RESPONSE_ID ++);
    ns.setNodeHealthStatus(NodeHealthStatus.newInstance(true, "", 0));
    ns.setOpportunisticContainersStatus(opportunisticContainersStatus);
    beatRequest.setNodeStatus(ns);
    
    NodeHeartbeatResponse beatResponse =
        rm.getResourceTrackerService().nodeHeartbeat(beatRequest);
    if (! beatResponse.getContainersToCleanup().isEmpty()) {
      // remove from queue
      synchronized(releasedContainerList) {
        for (ContainerId containerId : beatResponse.getContainersToCleanup()){
          if (amContainerList.contains(containerId)) {
            // AM container (not killed?, only release)
            synchronized(amContainerList) {
              amContainerList.remove(containerId);
            }
            LOG.debug(MessageFormat.format("NodeManager {0} releases " +
                "an AM ({1}).", node.getNodeID(), containerId));
          } else {
        	boolean found=false;  
        	//the container is queued, remove it from the queue 
        	synchronized(oppContainerQueuing){
        		  Iterator<Entry<Long, ContainerSimulator>> entryIt = oppContainerQueuing.entrySet().iterator();
        		  while(entryIt.hasNext()){
        			  Entry<Long, ContainerSimulator> entry = entryIt.next();
        			  ContainerSimulator cs=entry.getValue();
        			 if(cs.getId().equals(containerId)){
        		    	 //remove from the queue.
        		    	 entryIt.remove();
        		         found=true;
        		         break;
        		      }
        		  }
        	}
        	//if it is not queued, remove it from running queue
        	if(!found && runningContainers.containsKey(containerId))
        	   removeContainer(runningContainers.get(containerId));
          }
        }
      }
    }
    if (beatResponse.getNodeAction() == NodeAction.SHUTDOWN) {
      lastStep();
    }
  }

  @Override
  public void lastStep() {
    // do nothing
  }

  /**
   * catch status of all containers located on current node
   */
  private ArrayList<ContainerStatus> generateContainerStatusList() {
    ArrayList<ContainerStatus> csList = new ArrayList<ContainerStatus>();
    // add running containers
    for (ContainerSimulator container : runningContainers.values()) {
      csList.add(newContainerStatus(container.getId(),
        ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
    }
    synchronized(amContainerList) {
      for (ContainerId cId : amContainerList) {
        csList.add(newContainerStatus(cId,
            ContainerState.RUNNING, ContainerExitStatus.SUCCESS));
      }
    }
    
    // add complete containers
    synchronized(completedContainerList) {
      for (ContainerId cId : completedContainerList) {
        LOG.debug(MessageFormat.format("NodeManager {0} completed" +
                " container ({1}).", node.getNodeID(), cId));
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.SUCCESS));
      }
      completedContainerList.clear();
    }
    
    // released containers
    synchronized(releasedContainerList) {
      for (ContainerId cId : releasedContainerList) {
        LOG.debug(MessageFormat.format("NodeManager {0} released container" +
                " ({1}).", node.getNodeID(), cId));
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.ABORTED));
      }
      releasedContainerList.clear();
    }
    
    // killed containers
    synchronized(killedContainerList) {
      for (ContainerId cId : killedContainerList) {
        LOG.info(MessageFormat.format("NodeManager {0} killed container" +
                " ({1}).", node.getNodeID(), cId));
        csList.add(newContainerStatus(
                cId, ContainerState.COMPLETE, ContainerExitStatus.KILLED_BY_CONTAINER_SCHEDULER));
      }
      killedContainerList.clear();
    }
    return csList;
  }

  private ContainerStatus newContainerStatus(ContainerId cId, 
                                             ContainerState state,
                                             int exitState) {
    ContainerStatus cs = Records.newRecord(ContainerStatus.class);
    cs.setContainerId(cId);
    cs.setState(state);
    cs.setExitStatus(exitState);
    return cs;
  }

  public RMNode getNode() {
    return node;
  }
  
  /**
   * launch a new container with the given life time
   */
  public void addNewContainer(Container container, long lifeTimeMS,
		   ExecutionTypeRequest exeType, 
		  List<Long> times,List<Long> memories, long appLaunchTime){
   
    if (lifeTimeMS != -1) {
      // normal container
      // Note, the contianer may be queued, so we do not set the true endTime yet. 
      ContainerSimulator cs = new ContainerSimulator(container.getId(),
              container.getResource(), -1,
              lifeTimeMS,exeType,times,memories);
     
      if(cs.getExeType().getExecutionType().equals(ExecutionType.OPPORTUNISTIC)){
    	 if(checkMemoryAvailable(cs) > 0){
    		 //launch this container
             luanchContainer(cs);
         		 
    	 }else{
    		 //queuing this container within the limitation of the queu
    		 if(oppContainerQueuing.size() < queuingLimit){
    		 //queuing this container
    		 LOG.info("queuing container "+container.getId()+" on node "+node.getNodeID()+
    				  " phy utilization " +nodeUtilization.getPhysicalMemory()+
    				  " vir utilization " +nodeUtilization.getVirtualMemory()+
    				  " queuing size "    +oppContainerQueuing.size());
    		 synchronized(oppContainerQueuing){
    			//avoid launch time collision
    			while(oppContainerQueuing.containsKey(appLaunchTime))
    				 appLaunchTime++;
    		    oppContainerQueuing.put(appLaunchTime, cs);
    		 }
    		 
    		//kill this container
    		}else{
    		  LOG.info("killing container because queue is full "+container.getId()+" on node "+node.getNodeID()+
      				  " phy utilization "+nodeUtilization.getPhysicalMemory()+
      				  " vir utilization "+nodeUtilization.getVirtualMemory());	
    		  synchronized(killedContainerList){
    			 killedContainerList.add(cs.getId());
    		  } 
    		}
    	 }
    	//for regular container, no matter how, we need to run it.	 
      }else{
    	 long insufficient=checkMemoryAvailable(cs);
         if(insufficient < 0){
        	 killContainersToFreeMemory(-insufficient);
         }
         luanchContainer(cs);
      }
    } else {
      // AM container
      // -1 means AMContainer
      synchronized(amContainerList) {
    	//wo do not account resources for am container
        amContainerList.add(container.getId());
      }
    }
  }

  /**
   * clean up an AM container and add to completed list
   * @param containerId id of the container to be cleaned
   */
  public void cleanupContainer(ContainerId containerId) {
    synchronized(amContainerList) {
      amContainerList.remove(containerId);
    }
    synchronized(completedContainerList) {
      completedContainerList.add(containerId);
    }
  }

  @VisibleForTesting
  Map<ContainerId, ContainerSimulator> getRunningContainers() {
    return runningContainers;
  }

  @VisibleForTesting
  List<ContainerId> getAMContainers() {
    return amContainerList;
  }

  @VisibleForTesting
  List<ContainerId> getCompletedContainers() {
    return completedContainerList;
  }
}
