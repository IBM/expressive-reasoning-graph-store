FROM tomcat:7.0.96

MAINTAINER Ankita Likhyani

#ENV RDF4J_VERSION="3.0.0" 
#ENV RDF4J_DATA="/opt/eclipse-rdf4j-${RDF4J_VERSION}/data" 
#ENV JVM_PARAMS="-Xmx4g" 

##RUN curl -sS -o /tmp/rdf4j.zip -L http://download.eclipse.org/rdf4j/eclipse-rdf4j-${RDF4J_VERSION}-sdk.zip && \
#    cd /opt && \ 
#    unzip /tmp/rdf4j.zip && \
#    rm /tmp/rdf4j.zip

#RUN mv /opt/eclipse-rdf4j-${RDF4J_VERSION}/war/*.war /usr/local/tomcat/webapps

#RUN echo "CATALINA_OPTS=\"\$CATALINA_OPTS \$JVM_PARAMS -Dorg.eclipse.rdf4j.appdata.basedir=\$RDF4J_DATA\"" >> /usr/local/tomcat/bin/setenv.sh

#VOLUME ${RDF4J_DATA}

COPY . /usr/local/tomcat/

ARG MAVEN_VERSION=3.6.3

ARG USER_HOME_DIR="/root"

RUN mkdir -p /usr/share/maven && \
curl -fsSL http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar -xzC /usr/share/maven --strip-components=1 && \
ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven

ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

ENV MAVEN_OPTS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"

#RUN apt-get update && apt-get -y install openjdk-8-jdk && cp -rp /usr/lib/jvm/java-8-openjdk-amd64/lib/tools.jar /usr/lib/jvm/java-8-openjdk-amd64/jre/lib/tools.jar
RUN mvn clean install -Dmaven.test.skip=true 
#&& mvn package -Dmaven.test.skip=true

RUN cp -pr  /usr/local/tomcat/RDF4J/rdf4j-workbench/target/rdf4j-workbench-0.0.1-SNAPSHOT.war /usr/local/tomcat/webapps/rdf4j-workbench.war

RUN cp -rp /usr/local/tomcat/RDF4J/rdf4j-server/target/rdf4j-server-0.0.1-SNAPSHOT.war /usr/local/tomcat/webapps/rdf4j-server.war
EXPOSE 8080
