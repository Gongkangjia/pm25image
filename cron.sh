for i in {1..200000};do
mm=$(date +"%M")
if [ "$mm" = "35" ];then
python3 run.py 
else
echo "$mm"
fi
sleep 60
done

