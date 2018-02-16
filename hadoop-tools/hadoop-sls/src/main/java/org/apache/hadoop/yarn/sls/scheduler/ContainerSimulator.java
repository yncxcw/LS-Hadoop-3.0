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

package org.apache.hadoop.yarn.sls.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.log4j.Logger;

@Private
@Unstable
public class ContainerSimulator implements Delayed {
  // id
  private ContainerId id;
  // resource allocated
  private Resource resource;
  // end time
  private long endTime;
  // life time (ms)
  private long lifeTime;
  // host name
  private String hostname;
  // priority
  private int priority;
  // type 
  private String type;
  //mapped memory usage by sorted by time
  private List<Long> times;
  
  private List<Long> memories;
  
  private ExecutionTypeRequest exeType;

  private final static Logger LOG = Logger.getLogger(ContainerSimulator.class);

/**
   * invoked when AM schedules containers to allocate
   */
  public ContainerSimulator(Resource resource, long lifeTime,
      String hostname, int priority, String type, ExecutionTypeRequest exeType) {
    this.resource = resource;
    this.lifeTime = lifeTime;
    this.hostname = hostname;
    this.priority = priority;
    this.type = type;
    this.exeType=exeType;
    this.times=new ArrayList<Long>();
    this.times.add(lifeTime);
    this.memories=new ArrayList<Long>();
    this.memories.add(resource.getMemorySize());
    
    
  }
  
  /**
   * invoked when AM schedules containers to allocate
   */
  public ContainerSimulator(Resource resource, long lifeTime,
	      String hostname, int priority, String type,
	      ExecutionTypeRequest exeType,
	      List<Long> times,List<Long> memories){
	 this(resource,lifeTime,hostname,priority,type,exeType);
	 this.times=times;
	 this.memories=memories;
	 
	 
  }
  
  public long pullCurrentMemoryUsuage(long time){
	  
	  if(times == null || memories == null){
		  
		  LOG.warn("times null");
		  return 0;
	  }
	  
	  if(times.size() ==0 || memories.size() == 0){
		  LOG.warn("times 0");
		  return 0;
	  }
	  
	  //transform from second to millsecond
	  long runTime=(time-(endTime-lifeTime));
	  //LOG.info("runTime "+runTime+" endTime "+endTime+" lifeTime "+lifeTime);
	  
	  //for the first query of the memory usage
	  if(runTime <= 0)
		  return memories.get(0);
	  
	  int index=0;
	  for(;index<times.size();index++){
		  if(runTime<times.get(index)){
			  break;
		  }
	  }
	  
	  //for the queries of the memory usage after completion
	  if(index >= memories.size())
		  return memories.get(memories.size()-1);
	  
	  return memories.get(index);
  }
  
  
  /**
   * invoke when NM schedules containers to run
   */
  public ContainerSimulator(ContainerId id, Resource resource, long endTime,
      long lifeTime, ExecutionTypeRequest exeType,List<Long> times,List<Long> memories) {
    this.id = id;
    this.resource = resource;
    this.endTime = endTime;
    this.lifeTime = lifeTime;
    this.times=times;
    this.memories=memories;
    this.exeType=exeType;
  
  }
  
  public Resource getResource() {
    return resource;
  }
  
  public ContainerId getId() {
    return id;
  }

  @Override
  public int compareTo(Delayed o) {
    if (!(o instanceof ContainerSimulator)) {
      throw new IllegalArgumentException(
              "Parameter must be a ContainerSimulator instance");
    }
    
    ContainerSimulator other = (ContainerSimulator) o;
    if(endTime <= 0 || other.endTime <= 0){
    	throw new IllegalArgumentException(
                "endTime must be large than 0");	
    }
    
    return (int) Math.signum(endTime - other.endTime);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(endTime - System.currentTimeMillis(),
          TimeUnit.MILLISECONDS);
  }
  
  public long getLifeTime() {
    return lifeTime;
  }
  
  
  public String getHostname() {
    return hostname;
  }
  
  public void setEndTime(long time){
	 this.endTime = time; 
  }
  
  public long getEndTime() {
    return endTime;
  }
  
  public int getPriority() {
    return priority;
  }
  
  public String getType() {
    return type;
  }
  
  public void setPriority(int p) {
    priority = p;
  }
  
  
  public List<Long> getTimes() {
		return times;
  }

  public void setTimes(List<Long> times) {
		this.times = times;
  }
  
  public List<Long> getMemories() {
	return memories;
   }

  public void setMemories(List<Long> memories) {
	this.memories = memories;
  }

  public ExecutionTypeRequest getExeType() {
		return exeType;
  }

  public void setExeType(ExecutionTypeRequest exeType) {
		this.exeType = exeType;
  }
  
}
