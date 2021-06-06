JE provides a fully functional JMX MBean in com.sleepycat.je.jmx.JEMonitor.
To use this MBean, build and deploy jejmx.jar:

      1. cd <jehome>
      2. modify <jehome>/build.properties and set j2ee.jarfile to an 
         appropriate J2EE jar.
      3. ant jmx

This builds a jejmx.jar in <jehome>/build/lib which contains the
MBean. A sample JBoss service descriptor can be found in
je-jboss-service.xml in this directory. The MBean can be deployed
by modifying the service file to point to a JE environment, and
then copying the service file, jejmx.jar, and je.jar to the JBoss
deployment directory.

JEMonitor expects another component in the JVM to configure and open
the JE environment; it will only access a JE environment that is
already active. It is intended for these use cases:

-  The application wants to add database monitoring with minimal effort and
   little knowledge of JMX. Configuring JEMonitor within the JMX container
   provides monitoring without requiring application code changes. 

-  An application already supports JMX and wants to add database monitoring
   without modifying its existing MBean.  The user can configure JEMonitor in
   the JMX container in conjunction with other application MBeans that are
   non-overlapping with JE monitoring.  No application code changes are
   required. 

Users may want to incorporate JE management functionality into their
own MBeans, expecially if their application configures and opens the
JE environment. This can be done by using the utility class
com.sleepycat.je.jmx.JEMBeanHelper and an example implementation,
com.sleepycat.je.JEApplicationMBean which is provided in this
directory. This MBean differs from JEMonitor by supporting environment
configuration and creation from within the MBean. JEApplicationMBean
may be deployed, or used as a starting point for an alternate
implementation. To build the example,

      1. cd <jehome>
      2. modify <jehome>/build.properties and set j2ee.jarfile to an 
         appropriate J2EE jar.
      3. ant jmx-examples
 
This creates a jejmx-example.jar in <jehome>/build/lib that can be
copied to the appropriate deployment directory. See the 
je-jboss-service.xml file for an example of how this might be done for JBoss.
