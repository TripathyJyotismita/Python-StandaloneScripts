# /usr/bin
srcCln=$1
destCln=$2
library=$3
latestVersion=$4
specPath=$5
branch=$6
. ../.bashrc

echo 'Sourcing p4config'| tee -a output.log
. .p4config
echo 'Logging In as p4 user' | tee -a output.log
p4pass=`cat key.txt`
set P4PASSWD=$p4pass
p4login=`p4 login -s`
echo 'Syncing files - ' | tee -a output.log
p4 sync >> output.log
p4 change -o>changeDescription.txt
sed -i "/1-line/c\	Upgrade $library to $latestVersion version" changeDescription.txt
sed -i "/multi-line/c\		" changeDescription.txt
p4 change -i<changeDescription.txt>changenum_out.txt
value=$(<changenum_out.txt)
num=$(sed 's/[^0-9]//g' changenum_out.txt)
echo 'Changeset number : '$num |  tee -a output.log
echo 'File is open for edit' | tee -a output.log
p4 edit -c $num $specPath >> output.log
sed -i "s/$srcCln/$destCln/" $specPath
echo 'The files affected - ' | tee -a output.log
p4 diff | tee -a output.log
printf '%80s\n' | tr ' ' \#
echo "SANDBOX BUILD IN PROGRESS....... "
echo "gobuild sandbox queue vcenter --branch=$branch --changeset=$num --buildtype=beta --syncto=latest --no-store-trees">>output.log
gobuild sandbox queue vcenter --branch=$branch --changeset=$num --buildtype=beta --syncto=latest --no-store-trees &>outputfile1.out
cat outputfile1.out >> output.log
echo 'SANDBOX BUILD URL: '
grep -o "http[^ ]*" outputfile1.out | tee outputfile.txt
sed -i 's/[^0-9]//g' outputfile.txt

