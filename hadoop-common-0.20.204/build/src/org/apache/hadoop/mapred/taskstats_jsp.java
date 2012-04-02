package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.lang.String;
import java.text.*;
import java.util.*;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JSPUtil.JobWithViewAccessCheck;
import org.apache.hadoop.util.*;
import java.text.SimpleDateFormat;

public final class taskstats_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {

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

      out.write('\n');
      out.write('\n');

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String attemptid = request.getParameter("attemptid");
  TaskAttemptID attemptidObj = TaskAttemptID.forName(attemptid);
  // Obtain tipid for attemptId, if attemptId is available.
  TaskID tipidObj =
      (attemptidObj == null) ? TaskID.forName(request.getParameter("tipid"))
                             : attemptidObj.getTaskID();
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
  
  Format decimal = new DecimalFormat();
  Counters counters;
  if (attemptid == null) {
    counters = tracker.getTipCounters(tipidObj);
    attemptid = tipidObj.toString(); // for page title etc
  }
  else {
    TaskStatus taskStatus = tracker.getTaskStatus(attemptidObj);
    counters = taskStatus.getCounters();
  }

      out.write("\n\n<html>\n  <head>\n    <title>Counters for ");
      out.print(attemptid);
      out.write("</title>\n  </head>\n<body>\n<h1>Counters for ");
      out.print(attemptid);
      out.write("</h1>\n\n<hr>\n\n");

  if ( counters == null ) {

      out.write("\n    <h3>No counter information found for this task</h3>\n");

  } else {    

      out.write("\n    <table>\n");

      for (String groupName : counters.getGroupNames()) {
        Counters.Group group = counters.getGroup(groupName);
        String displayGroupName = group.getDisplayName();

      out.write("\n        <tr>\n          <td colspan=\"3\"><br/><b>\n          ");
      out.print(HtmlQuoting.quoteHtmlChars(displayGroupName));
      out.write("</b></td>\n        </tr>\n");

        for (Counters.Counter counter : group) {
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
