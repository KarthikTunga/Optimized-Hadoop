package org.apache.hadoop.hdfs.server.datanode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;
import java.text.DateFormat;

public final class browseBlock_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


  static JspHelper jspHelper = new JspHelper();

  public void generateFileDetails(JspWriter out, HttpServletRequest req,
                                  Configuration conf
                                 ) throws IOException, InterruptedException {

    int chunkSizeToView = 0;
    long startOffset = 0;
    int datanodePort;

    String blockIdStr = null;
    long currBlockId = 0;
    blockIdStr = req.getParameter("blockId");
    if (blockIdStr == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    currBlockId = Long.parseLong(blockIdStr);

    String datanodePortStr = req.getParameter("datanodePort");
    if (datanodePortStr == null) {
      out.print("Invalid input (datanodePort absent)");
      return;
    }
    datanodePort = Integer.parseInt(datanodePortStr);

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && 
        Integer.parseInt(chunkSizeToViewStr) > 0) {
     chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    } else {
     chunkSizeToView = JspHelper.getDefaultChunkSize(conf);
    }

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else startOffset = Long.parseLong(startOffsetStr);
    
    String filename = req.getParameter("filename");
    if (filename == null || filename.length() == 0) {
      out.print("Invalid input");
      return;
    }

    String blockSizeStr = req.getParameter("blockSize"); 
    long blockSize = 0;
    if (blockSizeStr == null || blockSizeStr.length() == 0) {
      out.print("Invalid input");
      return;
    } 
    blockSize = Long.parseLong(blockSizeStr);

    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    DFSClient dfs = JspHelper.getDFSClient(ugi, jspHelper.nameNodeAddr, conf);
    List<LocatedBlock> blocks = 
      dfs.namenode.getBlockLocations(filename, 0, Long.MAX_VALUE).getLocatedBlocks();
    //Add the various links for looking at the file contents
    //URL for downloading the full file
    String downloadUrl = "http://" + req.getServerName() + ":" +
                         + req.getServerPort() + "/streamFile"
                         + URLEncoder.encode(filename, "UTF-8")
                         + "?" + JspHelper.DELEGATION_PARAMETER_NAME
                         + "=" + tokenString;
    out.print("<a name=\"viewOptions\"></a>");
    out.print("<a href=\"" + downloadUrl + "\">Download this file</a><br>");
    
    DatanodeInfo chosenNode;
    //URL for TAIL 
    LocatedBlock lastBlk = blocks.get(blocks.size() - 1);
    long blockId = lastBlk.getBlock().getBlockId();
    try {
      chosenNode = jspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }
    String fqdn = 
           InetAddress.getByName(chosenNode.getHost()).getCanonicalHostName();
    String tailUrl = "http://" + fqdn + ":" +
                     chosenNode.getInfoPort() + 
                 "/tail.jsp?filename=" + URLEncoder.encode(filename, "UTF-8") +
                 "&namenodeInfoPort=" + namenodeInfoPort +
                 "&chunkSizeToView=" + chunkSizeToView +
                 "&referrer=" + 
          URLEncoder.encode(req.getRequestURL() + "?" + req.getQueryString(),
                            "UTF-8") +
                 JspHelper.getDelegationTokenUrlParam(tokenString);
    out.print("<a href=\"" + tailUrl + "\">Tail this file</a><br>");

    out.print("<form action=\"/browseBlock.jsp\" method=GET>");
    out.print("<b>Chunk size to view (in bytes, up to file's DFS block size): </b>");
    out.print("<input type=\"hidden\" name=\"blockId\" value=\"" + currBlockId +
              "\">");
    out.print("<input type=\"hidden\" name=\"blockSize\" value=\"" + 
              blockSize + "\">");
    out.print("<input type=\"hidden\" name=\"startOffset\" value=\"" + 
              startOffset + "\">");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + filename +
              "\">");
    out.print("<input type=\"hidden\" name=\"datanodePort\" value=\"" + 
              datanodePort+ "\">");
    out.print("<input type=\"hidden\" name=\"namenodeInfoPort\" value=\"" +
              namenodeInfoPort + "\">");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value=" +
              chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\">");
    out.print("</form>");
    out.print("<hr>"); 
    out.print("<a name=\"blockDetails\"></a>");
    out.print("<B>Total number of blocks: "+blocks.size()+"</B><br>");
    //generate a table and dump the info
    out.println("\n<table>");
    for (LocatedBlock cur : blocks) {
      out.print("<tr>");
      blockId = cur.getBlock().getBlockId();
      blockSize = cur.getBlock().getNumBytes();
      String blk = "blk_" + Long.toString(blockId);
      out.print("<td>"+Long.toString(blockId)+":</td>");
      DatanodeInfo[] locs = cur.getLocations();
      for(int j=0; j<locs.length; j++) {
        String datanodeAddr = locs[j].getName();
        datanodePort = Integer.parseInt(datanodeAddr.substring(
                                        datanodeAddr.indexOf(':') + 1, 
                                    datanodeAddr.length())); 
        fqdn = InetAddress.getByName(locs[j].getHost()).getCanonicalHostName();
        String blockUrl = "http://"+ fqdn + ":" +
                        locs[j].getInfoPort() +
                        "/browseBlock.jsp?blockId=" + Long.toString(blockId) +
                        "&blockSize=" + blockSize +
               "&filename=" + URLEncoder.encode(filename, "UTF-8")+ 
                        "&datanodePort=" + datanodePort + 
                        "&genstamp=" + cur.getBlock().getGenerationStamp() + 
                        "&namenodeInfoPort=" + namenodeInfoPort +
                        "&chunkSizeToView=" + chunkSizeToView;
        out.print("<td>&nbsp</td>" 
          + "<td><a href=\"" + blockUrl + "\">" + datanodeAddr + "</a></td>");
      }
      out.println("</tr>");
    }
    out.println("</table>");
    out.print("<hr>");
    String namenodeHost = jspHelper.nameNodeAddr.getHostName();
    out.print("<br><a href=\"http://" + 
              InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
              namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

  public void generateFileChunks(JspWriter out, HttpServletRequest req,
                                 Configuration conf
                                ) throws IOException, InterruptedException {
    long startOffset = 0;
    int datanodePort = 0; 
    int chunkSizeToView = 0;

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    String filename = req.getParameter("filename");
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }
    
    String blockIdStr = null;
    long blockId = 0;
    blockIdStr = req.getParameter("blockId");
    if (blockIdStr == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    blockId = Long.parseLong(blockIdStr);

    String tokenString = req.getParameter(JspHelper.DELEGATION_PARAMETER_NAME);
    UserGroupInformation ugi = JspHelper.getUGI(req, conf);
    final DFSClient dfs = JspHelper.getDFSClient(ugi, jspHelper.nameNodeAddr,
                                                 conf);
    
    Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager.DUMMY_TOKEN;
    if (conf
        .getBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false)) {
      List<LocatedBlock> blks = dfs.namenode.getBlockLocations(filename, 0,
          Long.MAX_VALUE).getLocatedBlocks();
      if (blks == null || blks.size() == 0) {
        out.print("Can't locate file blocks");
        dfs.close();
        return;
      }
      for (int i = 0; i < blks.size(); i++) {
        if (blks.get(i).getBlock().getBlockId() == blockId) {
          accessToken = blks.get(i).getBlockToken();
          break;
        }
      }
    }
    
    String blockGenStamp = null;
    long genStamp = 0;
    blockGenStamp = req.getParameter("genstamp");
    if (blockGenStamp == null) {
      out.print("Invalid input (genstamp absent)");
      return;
    }
    genStamp = Long.parseLong(blockGenStamp);

    String blockSizeStr;
    long blockSize = 0;
    blockSizeStr = req.getParameter("blockSize"); 
    if (blockSizeStr == null) {
      out.print("Invalid input (blockSize absent)");
      return;
    }
    blockSize = Long.parseLong(blockSizeStr);
    
    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && Integer.parseInt(chunkSizeToViewStr) > 0)
      chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    else chunkSizeToView = JspHelper.getDefaultChunkSize(conf);

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else startOffset = Long.parseLong(startOffsetStr);

    String datanodePortStr = req.getParameter("datanodePort");
    if (datanodePortStr == null) {
      out.print("Invalid input (datanodePort absent)");
      return;
    }
    datanodePort = Integer.parseInt(datanodePortStr);
    out.print("<h3>File: ");
    JspHelper.printPathWithLinks(filename, out, namenodeInfoPort,
                                 tokenString);
    out.print("</h3><hr>");
    String parent = new File(filename).getParent();
    JspHelper.printGotoForm(out, namenodeInfoPort, tokenString, parent);
    out.print("<hr>");
    out.print("<a href=\"http://" + req.getServerName() + ":" + 
              req.getServerPort() + 
              "/browseDirectory.jsp?dir=" + 
              URLEncoder.encode(parent, "UTF-8") +
              "&namenodeInfoPort=" + namenodeInfoPort + 
              "\"><i>Go back to dir listing</i></a><br>");
    out.print("<a href=\"#viewOptions\">Advanced view/download options</a><br>");
    out.print("<hr>");

    //Determine the prev & next blocks
    long nextStartOffset = 0;
    long nextBlockSize = 0;
    String nextBlockIdStr = null;
    String nextGenStamp = null;
    String nextHost = req.getServerName();
    int nextPort = req.getServerPort();
    int nextDatanodePort = datanodePort;
    //determine data for the next link
    if (startOffset + chunkSizeToView >= blockSize) {
      //we have to go to the next block from this point onwards
      List<LocatedBlock> blocks = 
        dfs.namenode.getBlockLocations(filename, 0, Long.MAX_VALUE).getLocatedBlocks();
      for (int i = 0; i < blocks.size(); i++) {
        if (blocks.get(i).getBlock().getBlockId() == blockId) {
          if (i != blocks.size() - 1) {
            LocatedBlock nextBlock = blocks.get(i+1);
            nextBlockIdStr = Long.toString(nextBlock.getBlock().getBlockId());
            nextGenStamp = Long.toString(nextBlock.getBlock().getGenerationStamp());
            nextStartOffset = 0;
            nextBlockSize = nextBlock.getBlock().getNumBytes();
            DatanodeInfo d = jspHelper.bestNode(nextBlock);
            String datanodeAddr = d.getName();
            nextDatanodePort = Integer.parseInt(
                                      datanodeAddr.substring(
                                           datanodeAddr.indexOf(':') + 1, 
                                      datanodeAddr.length())); 
            nextHost = InetAddress.getByName(d.getHost()).getCanonicalHostName();
            nextPort = d.getInfoPort(); 
          }
        }
      }
    } 
    else {
      //we are in the same block
      nextBlockIdStr = blockIdStr;
      nextStartOffset = startOffset + chunkSizeToView;
      nextBlockSize = blockSize;
      nextGenStamp = blockGenStamp;
    }
    String nextUrl = null;
    if (nextBlockIdStr != null) {
      nextUrl = "http://" + nextHost + ":" + 
                nextPort + 
                "/browseBlock.jsp?blockId=" + nextBlockIdStr +
                "&blockSize=" + nextBlockSize + "&startOffset=" + 
                nextStartOffset + 
                "&genstamp=" + nextGenStamp +
                "&filename=" + URLEncoder.encode(filename, "UTF-8") +
                "&chunkSizeToView=" + chunkSizeToView + 
                "&datanodePort=" + nextDatanodePort +
                "&namenodeInfoPort=" + namenodeInfoPort +
                JspHelper.getDelegationTokenUrlParam(tokenString);
      out.print("<a href=\"" + nextUrl + "\">View Next chunk</a>&nbsp;&nbsp;");        
    }
    //determine data for the prev link
    String prevBlockIdStr = null;
    String prevGenStamp = null;
    long prevStartOffset = 0;
    long prevBlockSize = 0;
    String prevHost = req.getServerName();
    int prevPort = req.getServerPort();
    int prevDatanodePort = datanodePort;
    if (startOffset == 0) {
      List<LocatedBlock> blocks = 
        dfs.namenode.getBlockLocations(filename, 0, Long.MAX_VALUE).getLocatedBlocks();
      for (int i = 0; i < blocks.size(); i++) {
        if (blocks.get(i).getBlock().getBlockId() == blockId) {
          if (i != 0) {
            LocatedBlock prevBlock = blocks.get(i-1);
            prevBlockIdStr = Long.toString(prevBlock.getBlock().getBlockId());
            prevGenStamp = Long.toString(prevBlock.getBlock().getGenerationStamp());
            prevStartOffset = prevBlock.getBlock().getNumBytes() - chunkSizeToView;
            if (prevStartOffset < 0)
              prevStartOffset = 0;
            prevBlockSize = prevBlock.getBlock().getNumBytes();
            DatanodeInfo d = jspHelper.bestNode(prevBlock);
            String datanodeAddr = d.getName();
            prevDatanodePort = Integer.parseInt(
                                      datanodeAddr.substring(
                                          datanodeAddr.indexOf(':') + 1, 
                                      datanodeAddr.length())); 
            prevHost = InetAddress.getByName(d.getHost()).getCanonicalHostName();
            prevPort = d.getInfoPort();
          }
        }
      }
    }
    else {
      //we are in the same block
      prevBlockIdStr = blockIdStr;
      prevStartOffset = startOffset - chunkSizeToView;
      if (prevStartOffset < 0) prevStartOffset = 0;
      prevBlockSize = blockSize;
      prevGenStamp = blockGenStamp;
    }

    String prevUrl = null;
    if (prevBlockIdStr != null) {
      prevUrl = "http://" + prevHost + ":" + 
                prevPort + 
                "/browseBlock.jsp?blockId=" + prevBlockIdStr + 
                "&blockSize=" + prevBlockSize + "&startOffset=" + 
                prevStartOffset + 
                "&filename=" + URLEncoder.encode(filename, "UTF-8") + 
                "&chunkSizeToView=" + chunkSizeToView +
                "&genstamp=" + prevGenStamp +
                "&datanodePort=" + prevDatanodePort +
                "&namenodeInfoPort=" + namenodeInfoPort +
                JspHelper.getDelegationTokenUrlParam(tokenString);
      out.print("<a href=\"" + prevUrl + "\">View Prev chunk</a>&nbsp;&nbsp;");
    }
    out.print("<hr>");
    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" style=\"width:100%\" READONLY>");
    try {
    jspHelper.streamBlockInAscii(
            new InetSocketAddress(req.getServerName(), datanodePort), blockId, 
            accessToken, genStamp, blockSize, startOffset, chunkSizeToView, 
            out, conf);
    } catch (Exception e){
        out.print(e);
    }
    out.print("</textarea>");
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
      out.write('\n');
      out.write("\n<html>\n<head>\n");
JspHelper.createTitle(out, request, request.getParameter("filename")); 
      out.write("\n</head>\n<body onload=\"document.goto.dir.focus()\">\n");
 
   Configuration conf = 
     (Configuration) getServletContext().getAttribute(JspHelper.CURRENT_CONF);
   generateFileChunks(out, request, conf);

      out.write("\n<hr>\n");
 
   generateFileDetails(out, request, conf);

      out.write("\n\n<h2>Local logs</h2>\n<a href=\"/logs/\">Log</a> directory\n\n");

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
