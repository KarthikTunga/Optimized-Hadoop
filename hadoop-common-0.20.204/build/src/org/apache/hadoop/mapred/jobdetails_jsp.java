package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.text.*;
import java.util.*;
import java.text.DecimalFormat;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.mapred.TaskGraphServlet;

public final class jobdetails_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

	private static final long serialVersionUID = 1L;


 
  private void printTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                double completePercent,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    int failedTaskAttempts = 0;
    int killedTaskAttempts = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.wasKilled()) {
        killedTasks += 1;
      }
      failedTaskAttempts += task.numTaskFailures();
      killedTaskAttempts += task.numKilledTasks();
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print("<tr><th><a href=\"jobtasks.jsp?jobid=" + jobId + 
              "&type="+ kind + "&pagenum=1\">" + kind + 
              "</a></th><td align=\"right\">" + 
              StringUtils.formatPercent(completePercent, 2) +
              ServletUtil.percentageGraph((int)(completePercent * 100), 80) +
              "</td><td align=\"right\">" + 
              totalTasks + 
              "</td><td align=\"right\">" + 
              ((pendingTasks > 0) 
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=pending\">" + pendingTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((runningTasks > 0) 
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=running\">" + runningTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((finishedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                "&pagenum=1" + "&state=completed\">" + finishedTasks + "</a>" 
               : "0") + 
              "</td><td align=\"right\">" + 
              ((killedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind +
                "&pagenum=1" + "&state=killed\">" + killedTasks + "</a>"
               : "0") + 
              "</td><td align=\"right\">" + 
              ((failedTaskAttempts > 0) ? 
                  ("<a href=\"jobfailures.jsp?jobid=" + jobId + 
                   "&kind=" + kind + "&cause=failed\">" + failedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              " / " +
              ((killedTaskAttempts > 0) ? 
                  ("<a href=\"jobfailures.jsp?jobid=" + jobId + 
                   "&kind=" + kind + "&cause=killed\">" + killedTaskAttempts + 
                   "</a>") : 
                  "0"
                  ) + 
              "</td></tr>\n");
  }

  private void printJobLevelTaskSummary(JspWriter out,
                                String jobId,
                                String kind,
                                TaskInProgress[] tasks
                               ) throws IOException {
    int totalTasks = tasks.length;
    int runningTasks = 0;
    int finishedTasks = 0;
    int killedTasks = 0;
    for(int i=0; i < totalTasks; ++i) {
      TaskInProgress task = tasks[i];
      if (task.isComplete()) {
        finishedTasks += 1;
      } else if (task.isRunning()) {
        runningTasks += 1;
      } else if (task.isFailed()) {
        killedTasks += 1;
      }
    }
    int pendingTasks = totalTasks - runningTasks - killedTasks - finishedTasks; 
    out.print(((runningTasks > 0)  
               ? "<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                 "&pagenum=1" + "&state=running\">" + " Running" + 
                 "</a>" 
               : ((pendingTasks > 0) ? " Pending" :
                 ((finishedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind + 
                "&pagenum=1" + "&state=completed\">" + " Successful"
                 + "</a>" 
               : ((killedTasks > 0) 
               ?"<a href=\"jobtasks.jsp?jobid=" + jobId + "&type="+ kind +
                "&pagenum=1" + "&state=killed\">" + " Failed" 
                + "</a>" : "None")))));
  }
  
  private void printConfirm(JspWriter out, String jobId) throws IOException{
    String url = "jobdetails.jsp?jobid=" + jobId;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url+"\"></head>"
        + "<body><h3> Are you sure you want to kill " + jobId
        + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"kill\" />"
        + "<input type=\"submit\" name=\"kill\" value=\"Kill\" />"
        + "</form>"
        + "</td><td width=\"100\"><form method=\"post\" action=\"" + url
        + "\"><input type=\"submit\" value=\"Cancel\" name=\"Cancel\""
        + "/></form></td></tr></table></body></html>");
  }
  

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

      out.write('\n');
      out.write('\n');
      out.write('\n');
      out.write('\n');

  final JobTracker tracker = (JobTracker) application.getAttribute(
      "job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());

      out.write('\n');
      out.write("       \n");
   
    String jobId = request.getParameter("jobid"); 
    String refreshParam = request.getParameter("refresh");
    if (jobId == null) {
      out.println("<h2>Missing 'jobid'!</h2>");
      return;
    }
    
    int refresh = 60; // refresh every 60 seconds by default
    if (refreshParam != null) {
        try {
            refresh = Integer.parseInt(refreshParam);
        }
        catch (NumberFormatException ignored) {
        }
    }
    final JobID jobIdObj = JobID.forName(jobId);
    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobIdObj,
                                                     request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();

    final String newPriority = request.getParameter("prio");
    String user = request.getRemoteUser();
    UserGroupInformation ugi = null;
    if (user != null) {
      ugi = UserGroupInformation.createRemoteUser(user);
    }

    String action = request.getParameter("action");
    if(JSPUtil.privateActionsAllowed(tracker.conf) && 
        "changeprio".equalsIgnoreCase(action) 
        && request.getMethod().equalsIgnoreCase("POST")) {
      if (ugi != null) {
        try {
          ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws IOException{

              // checks job modify permission
              tracker.setJobPriority(jobIdObj, 
                  JobPriority.valueOf(newPriority));
              return null;
            }
          });
        } catch(AccessControlException e) {
          String errMsg = "User " + user + " failed to modify priority of " +
              jobIdObj + "!<br><br>" + e.getMessage() +
              "<hr><a href=\"jobdetails.jsp?jobid=" + jobId +
              "\">Go back to Job</a><br>";
          JSPUtil.setErrorAndForward(errMsg, request, response);
          return;
        }
      }
      else {// no authorization needed
        tracker.setJobPriority(jobIdObj,
             JobPriority.valueOf(newPriority));;
      }
    }
    
    if(JSPUtil.privateActionsAllowed(tracker.conf)) {
      action = request.getParameter("action");
      if(action!=null && action.equalsIgnoreCase("confirm")) {
        printConfirm(out, jobId);
        return;
      }
      else if(action != null && action.equalsIgnoreCase("kill") &&
          request.getMethod().equalsIgnoreCase("POST")) {
        if (ugi != null) {
          try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                // checks job modify permission
                tracker.killJob(jobIdObj);// checks job modify permission
                return null;
              }
            });
          } catch(AccessControlException e) {
            String errMsg = "User " + user + " failed to kill " + jobIdObj +
                "!<br><br>" + e.getMessage() +
                "<hr><a href=\"jobdetails.jsp?jobid=" + jobId +
                "\">Go back to Job</a><br>";
            JSPUtil.setErrorAndForward(errMsg, request, response);
            return;
          }
        }
        else {// no authorization needed
          tracker.killJob(jobIdObj);
        }
      }
    }

      out.write("\n\n\n<html>\n<head>\n  ");
 
  if (refresh != 0) {
      
      out.write("\n      <meta http-equiv=\"refresh\" content=\"");
      out.print(refresh);
      out.write("\">\n      ");

  }
  
      out.write("\n<title>Hadoop ");
      out.print(jobId);
      out.write(" on ");
      out.print(trackerName);
      out.write("</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n<h1>Hadoop ");
      out.print(jobId);
      out.write(" on <a href=\"jobtracker.jsp\">");
      out.print(trackerName);
      out.write("</a></h1>\n\n");
 
    if (job == null) {
      String historyFile = JobHistory.getHistoryFilePath(jobIdObj);
      if (historyFile == null) {
        out.println("<h2>Job " + jobId + " not known!</h2>");
        return;
      }
      String historyUrl = JobHistoryServer.getHistoryUrlPrefix(tracker.conf) +
          "/jobdetailshistory.jsp?logFile=" +
          JobHistory.JobInfo.encodeJobHistoryFilePath(historyFile);
      response.sendRedirect(response.encodeRedirectURL(historyUrl));
      return;
    }
    JobProfile profile = job.getProfile();
    JobStatus status = job.getStatus();
    int runState = status.getRunState();
    int flakyTaskTrackers = job.getNoOfBlackListedTrackers();
    out.print("<b>User:</b> " +
        HtmlQuoting.quoteHtmlChars(profile.getUser()) + "<br>\n");
    out.print("<b>Job Name:</b> " +
        HtmlQuoting.quoteHtmlChars(profile.getJobName()) + "<br>\n");
    out.print("<b>Job File:</b> <a href=\"jobconf.jsp?jobid=" + jobId + "\">" +
        profile.getJobFile() + "</a><br>\n");
    out.print("<b>Submit Host:</b> " +
        HtmlQuoting.quoteHtmlChars(job.getJobSubmitHostName()) + "<br>\n");
    out.print("<b>Submit Host Address:</b> " +
        HtmlQuoting.quoteHtmlChars(job.getJobSubmitHostAddress()) + "<br>\n");

    Map<JobACL, AccessControlList> jobAcls = status.getJobACLs();
    JSPUtil.printJobACLs(tracker, jobAcls, out);
    out.print("<b>Job Setup:</b>");
    printJobLevelTaskSummary(out, jobId, "setup", 
                             job.getTasks(TaskType.JOB_SETUP));
    out.print("<br>\n");
    if (runState == JobStatus.RUNNING) {
      out.print("<b>Status:</b> Running<br>\n");
      out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
      out.print("<b>Running for:</b> " + StringUtils.formatTimeDiff(
          System.currentTimeMillis(), job.getStartTime()) + "<br>\n");
    } else {
      if (runState == JobStatus.SUCCEEDED) {
        out.print("<b>Status:</b> Succeeded<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Finished at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Finished in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.FAILED) {
        out.print("<b>Status:</b> Failed<br>\n");
        out.print("<b>Failure Info:</b>" + 
                   HtmlQuoting.quoteHtmlChars(status.getFailureInfo()) + "<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Failed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Failed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      } else if (runState == JobStatus.KILLED) {
        out.print("<b>Status:</b> Killed<br>\n");
        out.print("<b>Failure Info:</b>" + 
                   HtmlQuoting.quoteHtmlChars(status.getFailureInfo()) + "<br>\n");
        out.print("<b>Started at:</b> " + new Date(job.getStartTime()) + "<br>\n");
        out.print("<b>Killed at:</b> " + new Date(job.getFinishTime()) +
                  "<br>\n");
        out.print("<b>Killed in:</b> " + StringUtils.formatTimeDiff(
            job.getFinishTime(), job.getStartTime()) + "<br>\n");
      }
    }
    out.print("<b>Job Cleanup:</b>");
    printJobLevelTaskSummary(out, jobId, "cleanup", 
                             job.getTasks(TaskType.JOB_CLEANUP));
    out.print("<br>\n");
    if (flakyTaskTrackers > 0) {
      out.print("<b>Black-listed TaskTrackers:</b> " + 
          "<a href=\"jobblacklistedtrackers.jsp?jobid=" + jobId + "\">" +
          flakyTaskTrackers + "</a><br>\n");
    }
    if (job.getSchedulingInfo() != null) {
      out.print("<b>Job Scheduling information: </b>" +
          job.getSchedulingInfo().toString() +"\n");
    }
    out.print("<hr>\n");
    out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");
    out.print("<tr><th>Kind</th><th>% Complete</th><th>Num Tasks</th>" +
              "<th>Pending</th><th>Running</th><th>Complete</th>" +
              "<th>Killed</th>" +
              "<th><a href=\"jobfailures.jsp?jobid=" + jobId + 
              "\">Failed/Killed<br>Task Attempts</a></th></tr>\n");
    printTaskSummary(out, jobId, "map", status.mapProgress(), 
                     job.getTasks(TaskType.MAP));
    printTaskSummary(out, jobId, "reduce", status.reduceProgress(),
                     job.getTasks(TaskType.REDUCE));
    out.print("</table>\n");
    
    
      out.write("\n    <p/>\n    <table border=2 cellpadding=\"5\" cellspacing=\"2\">\n    <tr>\n      <th><br/></th>\n      <th>Counter</th>\n      <th>Map</th>\n      <th>Reduce</th>\n      <th>Total</th>\n    </tr>\n    ");

    boolean isFine = true;
    Counters mapCounters = new Counters();
    isFine = job.getMapCounters(mapCounters);
    mapCounters = (isFine? mapCounters: new Counters());
    Counters reduceCounters = new Counters();
    isFine = job.getReduceCounters(reduceCounters);
    reduceCounters = (isFine? reduceCounters: new Counters());
    Counters totalCounters = new Counters();
    isFine = job.getCounters(totalCounters);
    totalCounters = (isFine? totalCounters: new Counters());
        
    for (String groupName : totalCounters.getGroupNames()) {
      Counters.Group totalGroup = totalCounters.getGroup(groupName);
      Counters.Group mapGroup = mapCounters.getGroup(groupName);
      Counters.Group reduceGroup = reduceCounters.getGroup(groupName);
      
      Format decimal = new DecimalFormat();
      
      boolean isFirst = true;
      for (Counters.Counter counter : totalGroup) {
        String name = counter.getDisplayName();
        String mapValue = decimal.format(mapGroup.getCounter(name));
        String reduceValue = decimal.format(reduceGroup.getCounter(name));
        String totalValue = decimal.format(counter.getCounter());
        
      out.write("\n        <tr>\n          ");

          if (isFirst) {
            isFirst = false;
            
      out.write("\n            <td rowspan=\"");
      out.print(totalGroup.size());
      out.write("\">\n            ");
      out.print(HtmlQuoting.quoteHtmlChars(totalGroup.getDisplayName()));
      out.write("</td>\n            ");

          }
          
      out.write("\n          <td>");
      out.print(HtmlQuoting.quoteHtmlChars(name));
      out.write("</td>\n          <td align=\"right\">");
      out.print(mapValue);
      out.write("</td>\n          <td align=\"right\">");
      out.print(reduceValue);
      out.write("</td>\n          <td align=\"right\">");
      out.print(totalValue);
      out.write("</td>\n        </tr>\n        ");

      }
    }
    
      out.write("\n    </table>\n\n<hr>Map Completion Graph - \n");

if("off".equals(request.getParameter("map.graph"))) {
  session.setAttribute("map.graph", "off");
} else if("on".equals(request.getParameter("map.graph"))){
  session.setAttribute("map.graph", "on");
}
if("off".equals(request.getParameter("reduce.graph"))) {
  session.setAttribute("reduce.graph", "off");
} else if("on".equals(request.getParameter("reduce.graph"))){
  session.setAttribute("reduce.graph", "on");
}

if("off".equals(session.getAttribute("map.graph"))) { 
      out.write("\n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&map.graph=on\" > open </a>\n");
} else { 
      out.write(" \n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&map.graph=off\" > close </a>\n<br><embed src=\"/taskgraph?type=map&jobid=");
      out.print(jobId);
      out.write("\" \n       width=\"");
      out.print(TaskGraphServlet.width + 2 * TaskGraphServlet.xmargin);
      out.write("\" \n       height=\"");
      out.print(TaskGraphServlet.height + 3 * TaskGraphServlet.ymargin);
      out.write("\"\n       style=\"width:100%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />\n");
}
      out.write('\n');
      out.write('\n');
if(job.getTasks(TaskType.REDUCE).length > 0) { 
      out.write("\n<hr>Reduce Completion Graph -\n");
if("off".equals(session.getAttribute("reduce.graph"))) { 
      out.write("\n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&reduce.graph=on\" > open </a>\n");
} else { 
      out.write(" \n<a href=\"/jobdetails.jsp?jobid=");
      out.print(jobId);
      out.write("&refresh=");
      out.print(refresh);
      out.write("&reduce.graph=off\" > close </a>\n \n <br><embed src=\"/taskgraph?type=reduce&jobid=");
      out.print(jobId);
      out.write("\" \n       width=\"");
      out.print(TaskGraphServlet.width + 2 * TaskGraphServlet.xmargin);
      out.write("\" \n       height=\"");
      out.print(TaskGraphServlet.height + 3 * TaskGraphServlet.ymargin);
      out.write("\" \n       style=\"width:100%\" type=\"image/svg+xml\" pluginspage=\"http://www.adobe.com/svg/viewer/install/\" />\n");
} }
      out.write("\n\n<hr>\n");
 if(JSPUtil.privateActionsAllowed(tracker.conf)) { 
      out.write("\n  <table border=\"0\"> <tr> <td>\n  Change priority from ");
      out.print(job.getPriority());
      out.write(" to:\n  <form action=\"jobdetails.jsp\" method=\"post\">\n  <input type=\"hidden\" name=\"action\" value=\"changeprio\"/>\n  <input type=\"hidden\" name=\"jobid\" value=\"");
      out.print(jobId);
      out.write("\"/>\n  </td><td> <select name=\"prio\"> \n  ");

    JobPriority jobPrio = job.getPriority();
    for (JobPriority prio : JobPriority.values()) {
      if(jobPrio != prio) {
        
      out.write(" <option value=");
      out.print(prio);
      out.write('>');
      out.print(prio);
      out.write("</option> ");

      }
    }
  
      out.write("\n  </select> </td><td><input type=\"submit\" value=\"Submit\"> </form></td></tr> </table>\n");
 } 
      out.write("\n\n<table border=\"0\"> <tr>\n    \n");
 if(JSPUtil.privateActionsAllowed(tracker.conf) 
    	&& runState == JobStatus.RUNNING) { 
      out.write("\n\t<br/><a href=\"jobdetails.jsp?action=confirm&jobid=");
      out.print(jobId);
      out.write("\"> Kill this job </a>\n");
 } 
      out.write("\n\n<hr>\n\n<hr>\n<a href=\"jobtracker.jsp\">Go back to JobTracker</a><br>\n");

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
