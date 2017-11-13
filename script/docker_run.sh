docker run -d -e "HOME=/home" -v $HOME/.aws:/home/.aws --env ETL_FILE=click_etl --name click_etl data_etl 


docker run -d -e "HOME=/home" -v $HOME/.aws:/home/.aws --env ETL_FILE=etl_event --name etl_event 643619091312.dkr.ecr.ap-northeast-1.amazonaws.com/track_etl:latest