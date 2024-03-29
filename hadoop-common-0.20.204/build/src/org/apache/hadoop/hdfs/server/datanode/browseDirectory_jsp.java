package org.apache.hadoop.hdfs.server.datanode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;
import java.text.DateFormat;

public final class browseDirectory_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  static JspHelper jspHelper = new JspHelper();
  
  public void generateDirectoryStructure( JspWriter out, 
                                          HttpServletRequest req,
                                          HttpServletResponse resp,
                                          Configuration conf
                                         ) throws IOException, 
                                                  InterruptedException {
    String dir = req.getParameter("dir");
    if (dir == null || dir.length() == 0) {
      out.print("Invalid input");
      return;
    }
    
    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);
    
    DFSClient dfs = JspHelper.getDFSClient(ugi, jspHelper.nameNodeAddr, 
                                           conf);
    String target = dir;
    if (!dfs.exists(target)) {
      out.print("<h3>File or directory : " + target + " does not exist</h3>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, target);
    }
    else {
      if( !dfs.isDirectory(target) ) { // a file
        List<LocatedBlock> blocks = 
          dfs.namenode.getBlockLocations(dir, 0, 1).getLocatedBlocks();
	      
        LocatedBlock firstBlock = null;
        DatanodeInfo [] locations = null;
        if (blocks.size() > 0) {
          firstBlock = blocks.get(0);
          locations = firstBlock.getLocations();
        }
        if (locations == null || locations.length == 0) {
          out.print("Empty file");
        } else {
          DatanodeInfo chosenNode = jspHelper.bestNode(firstBlock);
          String fqdn = InetAddress.getByName(chosenNode.getHost()).
            getCanonicalHostName();
          String datanodeAddr = chosenNode.getName();
          int datanodePort = Integer.parseInt(
                                              datanodeAddr.substring(
                                                                     datanodeAddr.indexOf(':') + 1, 
                                                                     datanodeAddr.length())); 
          String redirectLocation = "http://"+fqdn+":" +
            chosenNode.getInfoPort() + 
            "/browseBlock.jsp?blockId=" +
            firstBlock.getBlock().getBlockId() +
            "&blockSize=" + firstBlock.getBlock().getNumBytes() +
            "&genstamp=" + firstBlock.getBlock().getGenerationStamp() +
            "&filename=" + URLEncoder.encode(dir, "UTF-8") + 
            "&datanodePort="
            + datanodePort + "&namenodeInfoPort=" + namenodeInfoPort
            + JspHelper.getDelegationTokenUrlParam(tokenString);
          resp.sendRedirect(redirectLocation);
        }
        return;
      }
      // directory
      //generate a table and dump the info
      String [] headings = { "Name", "Type", "Size", "Replication", 
                              "Block Size", "Modification Time",
                              "Permission", "Owner", "Group" };
      out.print("<h3>Contents of directory ");
      JspHelper.printPathWithLinks(dir, out, namenodeInfoPort, tokenString);
      out.print("</h3><hr>");
      JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, dir);
      out.print("<hr>");
	
      File f = new File(dir);
      String parent;
      if ((parent = f.getParent()) != null)
        out.print("<a href=\"" + req.getRequestURL() + "?dir=" + parent +
                  "&namenodeInfoPort=" + namenodeInfoPort +
                  JspHelper.getDelegationTokenUrlParam(tokenString) +
                  "\">Go to parent directory</a><br>");

      DirectoryListing thisListing = dfs.listPaths(target, HdfsFileStatus.EMPTY_NAME);
      if (thisListing == null || thisListing.getPartialListing().length == 0) { 
        out.print("Empty directory");
      }
      else {
        jspHelper.addTableHeader(out);
        int row=0;
        jspHelper.addTableRow(out, headings, row++);
        String cols [] = new String[headings.length];
        do {
          HdfsFileStatus[] files = thisListing.getPartialListing();
          for (int i = 0; i < files.length; i++) {
            //Get the location of the first block of the file
            String localname = files[i].getLocalName();
            if (!files[i].isDir()) {
              cols[1] = "file";
              cols[2] = StringUtils.byteDesc(files[i].getLen());
              cols[3] = Short.toString(files[i].getReplication());
              cols[4] = StringUtils.byteDesc(files[i].getBlockSize());
            }
            else {
              cols[1] = "dir";
              cols[2] = "";
              cols[3] = "";
              cols[4] = "";
            }
            String datanodeUrl = req.getRequestURL()+"?dir="+
              URLEncoder.encode(files[i].getFullName(target), "UTF-8") + 
              "&namenodeInfoPort=" + namenodeInfoPort + 
              JspHelper.getDelegationTokenUrlParam(tokenString);
            cols[0] = "<a href=\""+datanodeUrl+"\">"+localname+"</a>";
            cols[5] = FsShell.dateForm.format(new Date((files[i].getModificationTime())));
            cols[6] = files[i].getPermission().toString();
            cols[7] = files[i].getOwner();
            cols[8] = files[i].getGroup();
            jspHelper.addTableRow(out, cols, row++);
          }
          if (!thisListing.hasMore()) {
            break;
          }
          thisListing = dfs.listPaths(target, thisListing.getLastName());
        } while (thisListing != null);
        jspHelper.addTableFooter(out);
      }
    } 
    String namenodeHost = jspHelper.nameNodeAddr.getHostName();
    out.print("<br><a href=\"http://" + 
              InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
              namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
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
      out.write("\n\n<html>\n<head>\n<style type=text/css>\n<!--\nbody \n  {\n  font-face:sanserif;\n  }\n-->\n</style>\n");
JspHelper.createTitle(out, request, request.getParameter("dir")); 
      out.write("\n</head>\n\n<body onload=\"document.goto.dir.focus()\">\n");
 
  Configuration conf = 
    (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
  try {
    generateDirectoryStructure(out,request,response,conf);
  }
  catch(IOException ioe) {
    String msg = ioe.getLocalizedMessage();
    int i = msg.indexOf("\n");
    if (i >= 0) {
      msg = msg.substring(0, i);
    }
    out.print("<h3>" + msg + "</h3>");
  }

      out.write("\n<hr>\n\n<h2>Local logs</h2>\n<a href=\"/logs/\">Log</a> directory\n\n");

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
