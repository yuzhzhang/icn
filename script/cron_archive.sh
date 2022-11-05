today=$(date +"%y%m%d")
echo ${today}

mkdir -p ../arc/${today}
cp ../log/* ../arc/${today}/
cp ../cfg/* ../arc/${today}/
cp report.txt ../arc/${today}/

rm ../arc/${today}/icn.err
rm ../arc/${today}/icn.out

rm ../log/*

