package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import java.text.*;
import org.apache.hadoop.mapred.JobHistory.*;

public final class taskstatshistory_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

 private static SimpleDateFormat dateFormat = new SimpleDateFormat("d/MM HH:mm:ss") ;
    private static final long serialVersionUID = 1L;

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.apache.jasper.runtime.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");


/*
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

      out.write('\n');
      out.write('\n');
      out.write('\n');
      out.write('\n');

  String attemptid = request.getParameter("attemptid");
  if(attemptid == null) {
    out.println("No attemptid found! Pass a 'attemptid' parameter in the request.");
    return;
  }
  TaskID tipid = TaskAttemptID.forName(attemptid).getTaskID();
  String logFile = request.getParameter("logFile");
  String encodedLogFileName = 
    JobHistory.JobInfo.encodeJobHistoryFilePath(logFile);
  String jobid = JSPUtil.getJobID(new Path(encodedLogFileName).getName());
  Format decimal = new DecimalFormat();

  FileSystem fs = (FileSystem) application.getAttribute("fileSys");
  JobConf jobConf = (JobConf) application.getAttribute("jobConf");
  ACLsManager aclsManager = (ACLsManager) application.getAttribute("aclManager");
  JobHistory.JobInfo job = JSPUtil.checkAccessAndGetJobInfo(request,
      response, jobConf, aclsManager, fs, new Path(logFile));
  if (job == null) {
    return;
  }

  JobHistory.Task task = job.getAllTasks().get(tipid.toString());
  JobHistory.TaskAttempt attempt = task.getTaskAttempts().get(attemptid);

  Counters counters = 
    Counters.fromEscapedCompactString(attempt.get(Keys.COUNTERS));

      out.write("\n\n<html>\n  <head>\n    <title>Counters for ");
      out.print(attemptid);
      out.write("</title>\n  </head>\n<body>\n<h1>Counters for ");
      out.print(attemptid);
      out.write("</h1>\n\n<hr>\n\n");

  if (counters == null) {

      out.write("\n    <h3>No counter information found for this attempt</h3>\n");

  } else {    

      out.write("\n    <table>\n");

      for (String groupName : counters.getGroupNames()) {
        Counters.Group group = counters.getGroup(groupName);
        String displayGroupName = group.getDisplayName();

      out.write("\n        <tr>\n          <td colspan=\"3\"><br/><b>\n          ");
      out.print(HtmlQuoting.quoteHtmlChars(displayGroupName));
      out.write("</b></td>\n        </tr>\n");

        Iterator<Counters.Counter> ctrItr = group.iterator();
        while(ctrItr.hasNext()) {
          Counters.Counter counter = ctrItr.next();
          String displayCounterName = counter.getDisplayName();
          long value = counter.getCounter();

      out.write("\n          <tr>\n            <td width=\"50\"></td>\n            <td>");
      out.print(HtmlQuoting.quoteHtmlChars(displayCounterName));
      out.write("</td>\n            <td align=\"right\">");
      out.print(decimal.format(value));
      out.write("</td>\n          </tr>\n");

        }
      }

      out.write("\n    </table>\n");

  }

      out.write("\n\n<hr>\n<a href=\"jobdetailshistory.jsp?logFile=");
      out.print(encodedLogFileName);
      out.write("\">Go back to the job</a><br>\n<a href=\"jobhistoryhome.jsp\">Go back to Job History Viewer</a><br>\n");

out.println(ServletUtil.htmlFooter());

      out.write('\n');
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
