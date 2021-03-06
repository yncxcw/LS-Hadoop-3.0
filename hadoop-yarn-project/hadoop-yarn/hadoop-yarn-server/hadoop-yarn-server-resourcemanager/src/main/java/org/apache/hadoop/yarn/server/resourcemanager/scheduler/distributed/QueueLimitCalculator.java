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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor.ClusterNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor.LoadComparator;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class interacts with the NodeQueueLoadMonitor to keep track of the
 * mean and standard deviation of the configured metrics (queue length or queue
 * wait time) used to characterize the queue load of a specific node.
 * The NodeQueueLoadMonitor triggers an update (by calling the
 * <code>update()</code> method) every time it performs a re-ordering of
 * all nodes.
 */
public class QueueLimitCalculator {

  class Stats {
    private final AtomicInteger qmean = new AtomicInteger(0);
    private final AtomicInteger stdev = new AtomicInteger(0);
    private final AtomicInteger qsum   =new AtomicInteger(0);
    private final AtomicInteger qmax   = new AtomicInteger(0);
    private final AtomicInteger qmin   = new AtomicInteger(0);
    
    private final AtomicInteger  rmax   = new AtomicInteger(0);
    private final AtomicInteger  rmin   = new AtomicInteger(0);
    private final AtomicInteger  rmean   = new AtomicInteger(0);

    /**
     * Not thread safe. Caller should synchronize on sorted nodes list.
     */
    void update() {
      List<NodeId> sortedNodes = nodeSelector.getSortedNodes();
      if (sortedNodes.size() > 0) {
        //queuing stats
        int sum = 0;
        int max = 0;
        int min = 0;
        
        //running stats
        int r_max=0;
        int r_min=0;
        int r_sum=0;
        
        for (NodeId n : sortedNodes) {
        	
          //queuing metrics
          sum += getMetric(getNode(n));
          
          if(getMetric(getNode(n)) > max)
        	  max=getMetric(getNode(n));
          
          if(getMetric(getNode(n)) < min)
        	  min=getMetric(getNode(n));
          
          r_sum += getNode(n).runningLength;
          //running metrics
          if(getNode(n).runningLength > r_max)
        	  r_max=getNode(n).runningLength;
          
          if(getNode(n).runningLength < r_min)
        	  r_min=getNode(n).runningLength;
        	  
        }
        qmax.set(max);
        qmin.set(min);
        qsum.set(sum);
        qmean.set(sum / sortedNodes.size());
        
        rmax.set(r_max);
        rmin.set(r_min);
        rmean.set(r_sum/sortedNodes.size());

        // Calculate stdev
        int sqrSumMean = 0;
        for (NodeId n : sortedNodes) {
          int val = getMetric(getNode(n));
          sqrSumMean += Math.pow(val - qmean.get(), 2);
        }
        stdev.set(
            (int) Math.round(Math.sqrt(
                sqrSumMean / (float) sortedNodes.size())));
      }
      
      
    }

    private ClusterNode getNode(NodeId nId) {
      return nodeSelector.getClusterNodes().get(nId);
    }

    private int getMetric(ClusterNode cn) {
      return (cn != null) ? ((LoadComparator)nodeSelector.getComparator())
              .getMetric(cn) : 0;
    }

    public int getRMean(){
      return rmean.get();	
    }
    
    public int getRMax(){
      return rmax.get(); 	
    }
    
    public int getRMin(){
      return rmin.get();	
    }
    
    public int getMean() {
      return qmean.get();
    }

    public int getStdev() {
      return stdev.get();
    }
    
    public int getMax(){
      return qmax.get();	
    }
    
    public int getSum(){
      return qsum.get();	
    }
    
    public int getMin(){
      return qmin.get();	
    }
    
    
  }

  private final NodeQueueLoadMonitor nodeSelector;
  private final float sigma;
  private final int rangeMin;
  private final int rangeMax;
  private final Stats stats = new Stats();
  
  final static Log LOG = LogFactory.getLog(QueueLimitCalculator.class);

  QueueLimitCalculator(NodeQueueLoadMonitor selector, float sigma,
      int rangeMin, int rangeMax) {
    this.nodeSelector = selector;
    this.sigma = sigma;
    this.rangeMax = rangeMax;
    this.rangeMin = rangeMin;
  }

  private int determineThreshold() {
    return (int) (stats.getMean() + sigma * stats.getStdev());
  }

  void update() {
	LOG.info("queuing stats: "+stats.getSum() +"  "+stats.getMax() +"  "+stats.getMin()+"  "+stats.getMean());
    LOG.info("running stats: "+stats.getRMax()+"  "+stats.getRMin()+"  "+stats.getRMean());
	this.stats.update();
  }

  private int getThreshold() {
    int thres = determineThreshold();
    return Math.min(rangeMax, Math.max(rangeMin, thres));
  }

  public ContainerQueuingLimit createContainerQueuingLimit() {
    ContainerQueuingLimit containerQueuingLimit =
        ContainerQueuingLimit.newInstance();
    if (nodeSelector.getComparator() == LoadComparator.QUEUE_WAIT_TIME) {
      containerQueuingLimit.setMaxQueueWaitTimeInMs(getThreshold());
      containerQueuingLimit.setMaxQueueLength(-1);
    } else {
      containerQueuingLimit.setMaxQueueWaitTimeInMs(-1);
      containerQueuingLimit.setMaxQueueLength(getThreshold());
    }
    return containerQueuingLimit;
  }
}
