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

package org.apache.hadoop.yarn.sls.appmaster;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import org.apache.hadoop.yarn.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.sls.SLSRunner;
import org.apache.log4j.Logger;

@Private
@Unstable
public class MRAMSimulator extends AMSimulator {
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Maps are scheduled as soon as their requests are received. Reduces are
  scheduled when all maps have finished (not support slow-start currently).
  */
  
  private static final int PRIORITY_REDUCE = 10;
  private static final int PRIORITY_MAP = 20;
  
 
  // pending maps
  private LinkedList<ContainerSimulator> pendingMaps =
          new LinkedList<ContainerSimulator>();
  
  // pending failed maps
  private LinkedList<ContainerSimulator> pendingFailedMaps =
          new LinkedList<ContainerSimulator>();
  
  // scheduled maps
  private LinkedList<ContainerSimulator> scheduledMaps =
          new LinkedList<ContainerSimulator>();
  
  // assigned maps
  private Map<ContainerId, ContainerSimulator> assignedMaps =
          new HashMap<ContainerId, ContainerSimulator>();
  
  // reduces which are not yet scheduled
  private LinkedList<ContainerSimulator> pendingReduces =
          new LinkedList<ContainerSimulator>();
  
  // pending failed reduces
  private LinkedList<ContainerSimulator> pendingFailedReduces =
          new LinkedList<ContainerSimulator>();
 
  // scheduled reduces
  private LinkedList<ContainerSimulator> scheduledReduces =
          new LinkedList<ContainerSimulator>();
  
  // assigned reduces
  private Map<ContainerId, ContainerSimulator> assignedReduces =
          new HashMap<ContainerId, ContainerSimulator>();
  
  // all maps & reduces
  private LinkedList<ContainerSimulator> allMaps =
          new LinkedList<ContainerSimulator>();
  private LinkedList<ContainerSimulator> allReduces =
          new LinkedList<ContainerSimulator>();

  // counters
  private int mapFinished = 0;
  private int mapTotal = 0;
  private int reduceFinished = 0;
  private int reduceTotal = 0;
  // waiting for AM container 
  private boolean isAMContainerRunning = false;
  private Container amContainer;
  // finished
  private boolean isFinished = false;
  //fail application if kill to much
  private boolean killFailed = false;
  // resource for AM container
  private final static int MR_AM_CONTAINER_RESOURCE_MEMORY_MB = 1;
  private final static int MR_AM_CONTAINER_RESOURCE_VCORES =    1;

  public final Logger LOG = Logger.getLogger(MRAMSimulator.class);

  public void init(int id, int heartbeatInterval,
      List<ContainerSimulator> containerList, ResourceManager rm, SLSRunner se,
      long traceStartTime, long traceFinishTime, String user, String queue, 
      boolean isTracked, String oldAppId) {
    super.init(id, heartbeatInterval, containerList, rm, se, 
              traceStartTime, traceFinishTime, user, queue,
              isTracked, oldAppId);
    amtype = "mapreduce";
    
    // get map/reduce tasks
    for (ContainerSimulator cs : containerList) {
      if (cs.getType().equals("map")) {
        cs.setPriority(PRIORITY_MAP);
        pendingMaps.add(cs);
      } else if (cs.getType().equals("reduce")) {
        cs.setPriority(PRIORITY_REDUCE);
        pendingReduces.add(cs);
      }
    }
    allMaps.addAll(pendingMaps);
    allReduces.addAll(pendingReduces);
    mapTotal = pendingMaps.size();
    reduceTotal = pendingReduces.size();
    totalContainers = mapTotal + reduceTotal;
  }

  @Override
  public void firstStep() throws Exception {
    super.firstStep();
    
    requestAMContainer();
  }

  /**
   * send out request for AM container
   */
  protected void requestAMContainer()
          throws YarnException, IOException, InterruptedException {
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ResourceRequest amRequest = createResourceRequest(
            BuilderUtils.newResource(MR_AM_CONTAINER_RESOURCE_MEMORY_MB,
                    MR_AM_CONTAINER_RESOURCE_VCORES),
            ResourceRequest.ANY, 1, ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED), 1,REQUEST_ID);
    
    REQUEST_ID++;
    
    ask.add(amRequest);
    LOG.debug(MessageFormat.format("Application {0} sends out allocate " +
            "request for its AM", appId));
    final AllocateRequest request = this.createAllocateRequest(ask);

    UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
    Token<AMRMTokenIdentifier> token = rm.getRMContext().getRMApps()
            .get(appAttemptId.getApplicationId())
            .getRMAppAttempt(appAttemptId).getAMRMToken();
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
            new PrivilegedExceptionAction<AllocateResponse>() {
      @Override
      public AllocateResponse run() throws Exception {
        return rm.getApplicationMasterService().allocate(request);
      }
    });
    if (response != null) {
      responseQueue.put(response);
    }
  }

  
  protected ContainerSimulator matchAllocatedContainers(
		   List<ContainerSimulator> schedList, Container container){
	//first round, try to match both resource and locality
	NodeId rnode=container.getNodeId(); 
	//LOG.info("match node id: "+rnode.getHost()+" get resource "+container.getResource());
	for(int i=0;i<schedList.size();i++){
		 String rackHostNames[] = SLSUtils.getRackHostName(schedList.get(i).getHostname());
	     String hostName=rackHostNames[1];
	     //LOG.info("request resource "+schedList.get(i).getResource());
	     if(rnode.getHost().equals(hostName) && 
	    		 container.getResource().equals(schedList.get(i).getResource())){
	    	 return schedList.remove(i);
	     }
	} 
	//second round try resoruce.
	for(int i=0;i<schedList.size();i++){
	    if(container.getResource().equals(schedList.get(i).getResource())){
	    	return schedList.remove(i);
	    }
	}
	
	
    //no match give up this container	  
	return null;  
  }
  
  @Override
  @SuppressWarnings("unchecked")
  protected void processResponseQueue()
          throws InterruptedException, YarnException, IOException {
    // Check whether receive the am container
    if (!isAMContainerRunning) {
      if (!responseQueue.isEmpty()) {
        AllocateResponse response = responseQueue.take();
        if (response != null
            && !response.getAllocatedContainers().isEmpty()) {
          // Get AM container
          Container container = response.getAllocatedContainers().get(0);
          se.getNmMap().get(container.getNodeId())
              .addNewContainer(container, -1L,
            		       ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED),
            		        null,null,
            		        simulateStartTimeMS);
          // Start AM container
          amContainer = container;
          LOG.info(MessageFormat.format("Application {0} starts its " +
              "AM container ({1}) request size {2}.", appId, amContainer.getId(),containerSize));
          isAMContainerRunning = true;
          simulateAMStartTimeMS = System.currentTimeMillis() - 
                  SLSRunner.getRunner().getStartTimeMS();
        }
      }
      return;
    }

    while (! responseQueue.isEmpty()) {
      AllocateResponse response = responseQueue.take();

      // check completed containers
      if (! response.getCompletedContainersStatuses().isEmpty()) {
        for (ContainerStatus cs : response.getCompletedContainersStatuses()) {
          ContainerId containerId = cs.getContainerId();
          if (cs.getExitStatus() == ContainerExitStatus.SUCCESS) {
            if (assignedMaps.containsKey(containerId)) {
              LOG.info(MessageFormat.format("Application {0} has one" +
                      "mapper finished ({1}).", appId, containerId));
              assignedMaps.remove(containerId);
              mapFinished ++;
              finishedContainers ++;
            } else if (assignedReduces.containsKey(containerId)) {
              LOG.info(MessageFormat.format("Application {0} has one" +
                      "reducer finished ({1}).", appId, containerId));
              assignedReduces.remove(containerId);
              reduceFinished ++;
              finishedContainers ++;
            } else {
              // am container released event, this is am container finish event
              isFinished = true;
              LOG.info(MessageFormat.format("Application {0} goes to " +
                      "finish.", appId));
            }
          } else {
        	//statisc for killed tasks  
        	killedTask++;
        	LOG.info(MessageFormat.format("Application {0} has one " +
                    "task killed ({1}) total killed {2}.", appId, containerId,killedTask));
        	
        	//if all requeted containers are killed, we just failed the applications
        	if(killedTask >= 2*containerSize
        	   && isAMContainerRunning){
        	  LOG.info(MessageFormat.format("Application {0} reach the limit of killing time",appId));
        	  //kill all running containers
        	  //List<ContainerId> toKilledMap=new ArrayList<>(assignedMaps.keySet());
        	  
              killFailed=true;	
             //stop requesting new containers		
        	}
            // container to be killed
            if (assignedMaps.containsKey(containerId)) {
              ContainerSimulator containerSim=assignedMaps.remove(containerId);
              if(!killFailed)
                 pendingFailedMaps.add(containerSim);
            } else if (assignedReduces.containsKey(containerId)) {
              ContainerSimulator containerSim=assignedMaps.remove(containerId);
              if(!killFailed)
                pendingFailedReduces.add(containerSim);
            } else {
              LOG.info(MessageFormat.format("Application {0}'s AM is " +
                      "going to be killed. Restarting...", appId));
              restart();
            }
          }
        }
      }
      
     
      // check finished
      if (isAMContainerRunning &&
              (mapFinished == mapTotal) &&
              (reduceFinished == reduceTotal)) {
        // to release the AM container
        se.getNmMap().get(amContainer.getNodeId())
                .cleanupContainer(amContainer.getId());
        isAMContainerRunning = false;
        LOG.info(MessageFormat.format("Application {0} finish event " +
                "to clean up its AM container on application success", appId));
        isFinished = true;
        finalStatus= "SUCCESS";
        break;
      }
      
      //check if all assigned container finished
      if(isAMContainerRunning&&killFailed 
    		&& assignedMaps.isEmpty() 
    		&& assignedReduces.isEmpty()
        ){
    	  se.getNmMap().get(amContainer.getNodeId())
          .cleanupContainer(amContainer.getId());
    	  LOG.info(MessageFormat.format("Application {0} fail event " +
    	          "to clean up its AM container on application failure.", appId));
          isAMContainerRunning = false;
          isFinished = true;
          finalStatus= "FAILURE";
          break;  
      }

      //check for unsatisfied containers;
      if(scheduledMaps.size() > 0 || scheduledReduces.size() >0 ){
        LOG.info(MessageFormat.format("waiting for {0} maps {1} reduces",scheduledMaps.size(),scheduledReduces.size()));	  
      }
      // check allocated containers
      for (Container container : response.getAllocatedContainers()) {
        if (! scheduledMaps.isEmpty()) {
          ContainerSimulator cs = matchAllocatedContainers(scheduledMaps,container);
          if(cs==null){
        	  LOG.debug("allcoated resource could not find a match");
          }
          LOG.info(MessageFormat.format("Application {0} starts a " +
                  "launch a mapper ({1}) o node {2}", appId, container.getId(), container.getNodeId()));
          
          assignedMaps.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime(),
                		   cs.getExeType(),cs.getTimes(),cs.getMemories(),simulateStartTimeMS);
        } else if (! this.scheduledReduces.isEmpty()) {
          ContainerSimulator cs = matchAllocatedContainers(scheduledReduces,container);
          if(cs==null){
        	  LOG.warn("allcoated resource could not find a match");;
          }
          LOG.info(MessageFormat.format("Application {0} starts a " +
                  "launch a reducer ({1}).", appId, container.getId()));
          assignedReduces.put(container.getId(), cs);
          se.getNmMap().get(container.getNodeId())
                  .addNewContainer(container, cs.getLifeTime(),
                		 cs.getExeType(), cs.getTimes(),cs.getMemories(),simulateStartTimeMS);
        }
      }
    }
  }
  
  /**
   * restart running because of the am container killed
   */
  private void restart()
          throws YarnException, IOException, InterruptedException {
    // clear 
    finishedContainers = 0;
    isFinished = false;
    killFailed =false;
    killedTask = 0;
    mapFinished = 0;
    reduceFinished = 0;
    pendingFailedMaps.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    pendingFailedReduces.clear();
    pendingMaps.addAll(allMaps);
    pendingReduces.addAll(pendingReduces);
    isAMContainerRunning = false;
    amContainer = null;
    // resent am container request
    requestAMContainer();
  }

  @Override
  protected void sendContainerRequest()
          throws YarnException, IOException, InterruptedException {
	  
	   
    if (isFinished) {
      return;
    }
    
    // send out request
    List<ResourceRequest> ask = null;
    //if the application is still running, but we quit requesting more 
    //containers because of too much kill
    if (isAMContainerRunning && !killFailed) {
      if (mapFinished != mapTotal && mapTotal > 0) {
        // map phase
        if (! pendingMaps.isEmpty()) {
          ask = packageRequests(pendingMaps, PRIORITY_MAP);
          LOG.info(MessageFormat.format("Application {0} sends out " +
                  "request for {1} mappers.", appId, pendingMaps.size()));
          scheduledMaps.addAll(pendingMaps);
          pendingMaps.clear();
        } else if ( !pendingFailedMaps.isEmpty() && scheduledMaps.isEmpty()) {
          ask = packageRequests(pendingFailedMaps, PRIORITY_MAP);
          LOG.info(MessageFormat.format("Application {0} sends out " +
                  "requests for {1} failed mappers.", appId,
                  pendingFailedMaps.size()));
          scheduledMaps.addAll(pendingFailedMaps);
          pendingFailedMaps.clear();
        }
      } else if (reduceFinished != reduceTotal && reduceTotal > 0) {
        // reduce phase
        if (! pendingReduces.isEmpty()) {
          ask = packageRequests(pendingReduces, PRIORITY_REDUCE);
          LOG.info(MessageFormat.format("Application {0} sends out " +
                  "requests for {1} reducers.", appId, pendingReduces.size()));
          scheduledReduces.addAll(pendingReduces);
          pendingReduces.clear();
        } else if (! pendingFailedReduces.isEmpty()
                && scheduledReduces.isEmpty()) {
          ask = packageRequests(pendingFailedReduces, PRIORITY_REDUCE);
          LOG.info(MessageFormat.format("Application {0} sends out " +
                  "request for {1} failed reducers.", appId,
                  pendingFailedReduces.size()));
          scheduledReduces.addAll(pendingFailedReduces);
          pendingFailedReduces.clear();
        }
      }
    }
    if (ask == null) {
      ask = new ArrayList<ResourceRequest>();
    }
    
    final AllocateRequest request = createAllocateRequest(ask);
    if (totalContainers == 0) {
      request.setProgress(1.0f);
    } else {
      request.setProgress((float) finishedContainers / totalContainers);
    }

    UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
    
    RMApp rmApp=rm.getRMContext().getRMApps()
            .get(appAttemptId.getApplicationId());
    
    if(rmApp == null)
    	return;
    
    RMAppAttempt rmAppAttempt=rmApp.getRMAppAttempt(appAttemptId);
    
    if(rmAppAttempt == null)
    	return;
    
    
    Token<AMRMTokenIdentifier> token = rmAppAttempt.getAMRMToken();
    
    ugi.addTokenIdentifier(token.decodeIdentifier());
    AllocateResponse response = ugi.doAs(
            new PrivilegedExceptionAction<AllocateResponse>() {
      @Override
      public AllocateResponse run() throws Exception {
        return rm.getApplicationMasterService().allocate(request);
      }
    });
    if (response != null) {
      
      responseQueue.put(response);
    }
  }

  @Override
  protected void checkStop() {
    if (isFinished) {
      LOG.info(appId+"is going to stop in 1000ms");	
      super.setEndTime(System.currentTimeMillis()+1000);
    }
  }

  @Override
  public void lastStep() throws Exception {
    super.lastStep();

    // clear data structures
    allMaps.clear();
    allReduces.clear();
    assignedMaps.clear();
    assignedReduces.clear();
    pendingFailedMaps.clear();
    pendingFailedReduces.clear();
    pendingMaps.clear();
    pendingReduces.clear();
    scheduledMaps.clear();
    scheduledReduces.clear();
    responseQueue.clear();
  }
}
