package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.lang.String;
import java.util.*;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.AccessControlException;

public final class taskdetails_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d-MMM-yyyy HH:mm:ss");

	private static final long serialVersionUID = 1L;

private void printConfirm(JspWriter out,
      String attemptid, String action) throws IOException {
    String url = "taskdetails.jsp?attemptid=" + attemptid;
    out.print("<html><head><META http-equiv=\"refresh\" content=\"15;URL="
        + url + "\"></head>" + "<body><h3> Are you sure you want to kill/fail "
        + attemptid + " ?<h3><br><table border=\"0\"><tr><td width=\"100\">"
        + "<form action=\"" + url + "\" method=\"post\">"
        + "<input type=\"hidden\" name=\"action\" value=\"" + action + "\" />"
        + "<input type=\"submit\" name=\"Kill/Fail\" value=\"Kill/Fail\" />"
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

    final JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");

    String attemptid = request.getParameter("attemptid");
    final TaskAttemptID attemptidObj = TaskAttemptID.forName(attemptid);

    // Obtain tipid for attemptid, if attemptid is available.
    TaskID tipidObj =
        (attemptidObj == null) ? TaskID.forName(request.getParameter("tipid"))
                               : attemptidObj.getTaskID();
    if (tipidObj == null) {
      out.print("<b>tipid sent is not valid.</b><br>\n");
      return;
    }
    // Obtain jobid from tipid
    final JobID jobidObj = tipidObj.getJobID();
    String jobid = jobidObj.toString();

    JobWithViewAccessCheck myJob = JSPUtil.checkAccessAndGetJob(tracker, jobidObj,
        request, response);
    if (!myJob.isViewJobAllowed()) {
      return; // user is not authorized to view this job
    }

    JobInProgress job = myJob.getJob();
    if (job == null) {
      out.print("<b>Job " + jobid + " not found.</b><br>\n");
      return;
    }
    boolean privateActions = JSPUtil.privateActionsAllowed(tracker.conf);
    if (privateActions) {
      String action = request.getParameter("action");
      if (action != null) {
        String user = request.getRemoteUser();
        UserGroupInformation ugi = null;
        if (user != null) {
          ugi = UserGroupInformation.createRemoteUser(user);
        }
        if (action.equalsIgnoreCase("confirm")) {
          String subAction = request.getParameter("subaction");
          if (subAction == null)
            subAction = "fail-task";
          printConfirm(out, attemptid, subAction);
          return;
        }
        else if (action.equalsIgnoreCase("kill-task") 
            && request.getMethod().equalsIgnoreCase("POST")) {
          if (ugi != null) {
            try {
              ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                tracker.killTask(attemptidObj, false);// checks job modify permission
                return null;
              }
              });
            } catch(AccessControlException e) {
              String errMsg = "User " + user + " failed to kill task "
                  + attemptidObj + "!<br><br>" + e.getMessage() +
                  "<hr><a href=\"jobdetails.jsp?jobid=" + jobid +
                  "\">Go back to Job</a><br>";
              JSPUtil.setErrorAndForward(errMsg, request, response);
              return;
            }
          } else {// no authorization needed
            tracker.killTask(attemptidObj, false);
          }
          //redirect again so that refreshing the page will not attempt to rekill the task
          response.sendRedirect("/taskdetails.jsp?subaction=kill-task" +
              "&tipid=" + tipidObj.toString());
        }
        else if (action.equalsIgnoreCase("fail-task")
            && request.getMethod().equalsIgnoreCase("POST")) {
          if (ugi != null) {
            try {
              ugi.doAs(new PrivilegedExceptionAction<Void>() {
              public Void run() throws IOException{

                tracker.killTask(attemptidObj, true);// checks job modify permission
                return null;
              }
              });
            } catch(AccessControlException e) {
              String errMsg = "User " + user + " failed to fail task "
                  + attemptidObj + "!<br><br>" + e.getMessage() +
                  "<hr><a href=\"jobdetails.jsp?jobid=" + jobid +
                  "\">Go back to Job</a><br>";
              JSPUtil.setErrorAndForward(errMsg, request, response);
              return;
            }
          } else {// no authorization needed
            tracker.killTask(attemptidObj, true);
          }

          response.sendRedirect("/taskdetails.jsp?subaction=fail-task" +
              "&tipid=" + tipidObj.toString());
        }
      }
    }
    TaskInProgress tip = job.getTaskInProgress(tipidObj);
    TaskStatus[] ts = null;
    boolean isCleanupOrSetup = false;
    if (tip != null) { 
      ts = tip.getTaskStatuses();
      isCleanupOrSetup = tip.isJobCleanupTask();
      if (!isCleanupOrSetup) {
        isCleanupOrSetup = tip.isJobSetupTask();
      }
    }

      out.write("\n\n\n<html>\n<head>\n  <link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n  <title>Hadoop Task Details</title>\n</head>\n<body>\n<h1>Job <a href=\"/jobdetails.jsp?jobid=");
      out.print(jobid);
      out.write('"');
      out.write('>');
      out.print(jobid);
      out.write("</a></h1>\n\n<hr>\n\n<h2>All Task Attempts</h2>\n<center>\n");

    if (ts == null || ts.length == 0) {

      out.write("\n\t\t<h3>No Task Attempts found</h3>\n");

    } else {

      out.write("\n<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n<tr><td align=\"center\">Task Attempts</td><td>Machine</td><td>Status</td><td>Progress</td><td>Start Time</td> \n  ");

   if (!ts[0].getIsMap() && !isCleanupOrSetup) {
   
      out.write("\n<td>Shuffle Finished</td><td>Sort Finished</td>\n  ");

  }
  
      out.write("\n<td>Finish Time</td><td>Errors</td><td>Task Logs</td><td>Counters</td><td>Actions</td></tr>\n  ");

    for (int i = 0; i < ts.length; i++) {
      TaskStatus status = ts[i];
      String taskTrackerName = status.getTaskTracker();
      TaskTrackerStatus taskTracker = tracker.getTaskTrackerStatus(taskTrackerName);
      out.print("<tr><td>" + status.getTaskID() + "</td>");
      String taskAttemptTracker = null;
      String cleanupTrackerName = null;
      TaskTrackerStatus cleanupTracker = null;
      String cleanupAttemptTracker = null;
      boolean hasCleanupAttempt = false;
      if (tip != null && tip.isCleanupAttempt(status.getTaskID())) {
        cleanupTrackerName = tip.machineWhereCleanupRan(status.getTaskID());
        cleanupTracker = tracker.getTaskTrackerStatus(cleanupTrackerName);
        if (cleanupTracker != null) {
          cleanupAttemptTracker = "http://" + cleanupTracker.getHost() + ":"
            + cleanupTracker.getHttpPort();
        }
        hasCleanupAttempt = true;
      }
      out.print("<td>");
      if (hasCleanupAttempt) {
        out.print("Task attempt: ");
      }
      if (taskTracker == null) {
        out.print(taskTrackerName);
      } else {
        taskAttemptTracker = "http://" + taskTracker.getHost() + ":"
          + taskTracker.getHttpPort();
        out.print("<a href=\"" + taskAttemptTracker + "\">"
          + tracker.getNode(taskTracker.getHost()) + "</a>");
      }
      if (hasCleanupAttempt) {
        out.print("<br/>Cleanup Attempt: ");
        if (cleanupAttemptTracker == null ) {
          out.print(cleanupTrackerName);
        } else {
          out.print("<a href=\"" + cleanupAttemptTracker + "\">"
            + tracker.getNode(cleanupTracker.getHost()) + "</a>");
        }
      }
      out.print("</td>");
        out.print("<td>" + status.getRunState() + "</td>");
        out.print("<td>" + StringUtils.formatPercent(status.getProgress(), 2)
          + ServletUtil.percentageGraph(status.getProgress() * 100f, 80) + "</td>");
        out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getStartTime(), 0) + "</td>");
        if (!ts[i].getIsMap() && !isCleanupOrSetup) {
          out.print("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getShuffleFinishTime(), status.getStartTime()) + "</td>");
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getSortFinishTime(), status.getShuffleFinishTime())
          + "</td>");
        }
        out.println("<td>"
          + StringUtils.getFormattedTimeWithDiff(dateFormat, status
          .getFinishTime(), status.getStartTime()) + "</td>");

        out.print("<td><pre>");
        String [] failures = tracker.getTaskDiagnostics(status.getTaskID());
        if (failures == null) {
          out.print("&nbsp;");
        } else {
          for(int j = 0 ; j < failures.length ; j++){
            out.print(HtmlQuoting.quoteHtmlChars(failures[j]));
            if (j < (failures.length - 1)) {
              out.print("\n-------\n");
            }
          }
        }
        out.print("</pre></td>");
        out.print("<td>");
        String taskLogUrl = null;
        if (taskTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(taskTracker.getHost(),
        						String.valueOf(taskTracker.getHttpPort()),
        						status.getTaskID().toString());
      	}
        if (hasCleanupAttempt) {
          out.print("Task attempt: <br/>");
        }
        if (taskLogUrl == null) {
          out.print("n/a");
        } else {
          String tailFourKBUrl = taskLogUrl + "&start=-4097";
          String tailEightKBUrl = taskLogUrl + "&start=-8193";
          String entireLogUrl = taskLogUrl + "&all=true";
          out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
          out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
          out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
        }
        if (hasCleanupAttempt) {
          out.print("Cleanup attempt: <br/>");
          taskLogUrl = null;
          if (cleanupTracker != null ) {
        	taskLogUrl = TaskLogServlet.getTaskLogUrl(cleanupTracker.getHost(),
                                String.valueOf(cleanupTracker.getHttpPort()),
                                status.getTaskID().toString());
      	  }
          if (taskLogUrl == null) {
            out.print("n/a");
          } else {
            String tailFourKBUrl = taskLogUrl + "&start=-4097&cleanup=true";
            String tailEightKBUrl = taskLogUrl + "&start=-8193&cleanup=true";
            String entireLogUrl = taskLogUrl + "&all=true&cleanup=true";
            out.print("<a href=\"" + tailFourKBUrl + "\">Last 4KB</a><br/>");
            out.print("<a href=\"" + tailEightKBUrl + "\">Last 8KB</a><br/>");
            out.print("<a href=\"" + entireLogUrl + "\">All</a><br/>");
          }
        }
        out.print("</td><td>" + "<a href=\"/taskstats.jsp?attemptid=" +
          status.getTaskID() + "\">"
          + ((status.getCounters() != null) ? status.getCounters().size() : 0)
          + "</a></td>");
        out.print("<td>");
        if (privateActions
          && status.getRunState() == TaskStatus.State.RUNNING) {
        out.print("<a href=\"/taskdetails.jsp?action=confirm"
          + "&subaction=kill-task" + "&attemptid=" + status.getTaskID()
          + "\" > Kill </a>");
        out.print("<br><a href=\"/taskdetails.jsp?action=confirm"
          + "&subaction=fail-task" + "&attemptid=" + status.getTaskID()
          + "\" > Fail </a>");
        }
        else
          out.print("<pre>&nbsp;</pre>");
        out.println("</td></tr>");
      }
  
      out.write("\n</table>\n</center>\n\n");

      if (ts[0].getIsMap() && !isCleanupOrSetup) {

      out.write("\n<h3>Input Split Locations</h3>\n<table border=2 cellpadding=\"5\" cellspacing=\"2\">\n");

        for (String split: StringUtils.split(tracker.getTip(
                                         tipidObj).getSplitNodes())) {
          out.println("<tr><td>" + split + "</td></tr>");
        }

      out.write("\n</table>\n");
    
      }
    }

      out.write("\n\n<hr>\n<a href=\"jobdetails.jsp?jobid=");
      out.print(jobid);
      out.write("\">Go back to the job</a><br>\n<a href=\"jobtracker.jsp\">Go back to JobTracker</a><br>\n");

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
