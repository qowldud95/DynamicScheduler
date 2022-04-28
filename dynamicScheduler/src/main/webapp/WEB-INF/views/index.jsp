<%@ page language="java" contentType="text/html; charset=EUC-KR"
         pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>스케줄러 테스트</title>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
</head>
<body>
<h1>스케줄러 동작 시간 변경</h1>
<input type="text" id="cronTab">
<br/>
<a href="javascript:void(0);" id="changeTest">스케줄러 동작 시간 변경</a>
<a href="javascript:void(0);" id="pauseTest">스케줄러 중지</a>
</body>
<script>
    $(function(){
        $(document).on("click", "#changeTest", function(){
            $.ajax({
                url: 'updateScheduler.do',
                type: 'POST',
                data: {cron: $("#cronTab").val()},
                success: function (data) {
                    if(data.res == "success"){
                        alert("스케줄러 동작 시간이 변경되었습니다.");
                        location.reload();
                    }
                },
                error: function (error) {
                    console.log(error)
                }
            });
        }).on("click", "#pauseTest", function(){
            $.ajax({
                url: 'pauseScheduler.do',
                type: 'POST',
                data: '',
                success: function (data) {
                    if(data.res == "success"){
                        alert("스케줄러가 멈췄습니다.");
                        location.reload();
                    }
                },
                error: function (error) {
                    console.log(error)
                }
            });
        });
    });
</script>
</html>