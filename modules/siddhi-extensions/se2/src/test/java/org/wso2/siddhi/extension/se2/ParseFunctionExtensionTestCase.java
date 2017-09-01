/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.se2;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.se2.test.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

public class ParseFunctionExtensionTestCase {
    static final Logger log = Logger.getLogger(ParseFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testParseFunctionExtension1() throws InterruptedException {
        log.info("ParseFunctionExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (JobID string, RunDate string, DayofWeek string, startingtime string, endingtime string, runduration string);";
        String query = ("@info(name = 'query1') from inputStream select se2:parseJobID(JobID) as JobID, se2:parseRunDate(RunDate) as RunDate, se2:parseDayofWeek(DayofWeek) as DayofWeek, se2:parseTime(startingtime) as startingtime, se2:parseTime(endingtime) as endingtime,  se2:parseDuration(runduration) as runduration " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(1, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(2, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(3, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 4) {
                        Assert.assertEquals(1, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 5) {
                        Assert.assertEquals(2, event.getData(0));
                        eventArrived = true;
                    }
                    if (count.get() == 6) {
                        Assert.assertEquals(3, event.getData(0));
                        eventArrived = true;
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"JobID_1","1/29/2017","Sun","1/1/2017 2:40","1/1/2017 2:47","0:06:57"});
        inputHandler.send(new Object[]{"JobID_2","1/1/2017","Sun","1/1/2017 2:50","1/1/2017 3:15","0:24:10"});
        inputHandler.send(new Object[]{"JobID_3","1/1/2017","Sun","1/1/2017 3:15","1/1/2017 3:25","0:10:34"});
        inputHandler.send(new Object[]{"JobID_1","1/2/2017","Mon","1/2/2017 1:45","1/2/2017 2:17","0:31:57"});
        inputHandler.send(new Object[]{"JobID_2","1/2/2017","Mon","1/2/2017 3:05","1/2/2017 3:35","0:29:10"});
        inputHandler.send(new Object[]{"JobID_3","1/2/2017","Mon","1/2/2017 3:55","1/2/2017 4:15","0:20:34"});
        SiddhiTestHelper.waitForEvents(100, 6, count, 60000);
        Assert.assertEquals(6, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}
