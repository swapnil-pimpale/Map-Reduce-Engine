chmod 700 master_node.sh
chmod 700 slave_node.sh

cd framework-src
javac *.java
jar cvfe framework.jar Driver *.class
cp framework.jar ..
cd ..

cd wordcount-src
javac *.java
jar cvfe wordcount.jar WordCount *.class
cp wordcount.jar ..
cd ..

cd wordlength-src
javac *.java
jar cvfe wordlength.jar WordLength *.class
cp wordlength.jar ..
cd ..


