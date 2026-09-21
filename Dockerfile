FROM tomcat:9.0-jdk17-temurin

RUN rm -rf "${CATALINA_HOME}/webapps"/*

COPY docker/server.xml "${CATALINA_HOME}/conf/server.xml"
COPY docker/context.xml "${CATALINA_HOME}/conf/context.xml"
COPY target/api.war "${CATALINA_HOME}/webapps/api.war"

# The JDBC realm is loaded by Tomcat, outside the WAR classloader.
RUN mkdir /tmp/orca-war \
    && cd /tmp/orca-war \
    && jar xf "${CATALINA_HOME}/webapps/api.war" WEB-INF/lib/postgresql-42.6.0.jar \
    && mv WEB-INF/lib/postgresql-42.6.0.jar "${CATALINA_HOME}/lib/" \
    && rm -rf /tmp/orca-war
