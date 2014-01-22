
REMOTE=hadoopnest1.atla.twitter.com
rsync -vz /Users/myabandeh/repo/vulture/target/vulture-1.0-SNAPSHOT.jar $REMOTE:~/vulture/target/
rsync -avz /Users/myabandeh/repo/vulture/bin/ $REMOTE:~/vulture/bin/
rsync -avz /Users/myabandeh/repo/vulture/lib/ $REMOTE:~/vulture/lib/

