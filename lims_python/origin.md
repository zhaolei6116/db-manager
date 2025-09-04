

生信与康为商城接口文档




修订历史记录
日期	版本	说明	修改人
2024-07-01	1.0	初始化	JackieZhang
2024-08-09	1.1	增加get_key api 接口 	JackieZhang
2024-11-14	1.2	统计返回错误或者无报告格式	JackieZhang
2024-12-27	1.3	增加质粒总长度, 片段长度支持	JackieZhang
2025-04-24	1.4	增加异常状态	JackieZhang

 
目录
一、接口需求概述	3
二、接口设计	3
1.	获取密钥	3
2.	获取实验信息接口	4
3.	推送实验结果接口	6
4.	错误代码参考	8


 
一、接口需求概述
本文档提供Api接口供生信系统调用，用以自动同步结果，实验上传结果数据。

二、接口设计
所有接口通过标准https协议，post方式 raw数据传输，body使用json格式(Content-Type: application/json)， 
接口地址: 
https://www.cwbio.com/gwapi

1.	获取密钥
接口调用： /get_key

请求参数：
字段名称	字段说明	类型	必填	备注
appid	公钥	string	Y	四代测序项目cwbioprimecx
基因合成：cwbiogene
company	检验所简写	string	Y	支持检验所：B,T, X,S, G
请求示例：
{
  "appid": "cwbioprimecx",
  "company": "B"
}

返回参数（如果返回数据，需要加密）
中文名	字段名称	字段类型	备注
状态	code	String	200为成功 非200为不成功
信息提示	msg	String	成功信息提示
实验数据	data	String	成功的数据, 错误为空

返回数据示例(以北京健为为例)：
{
  "code": "200",
  "msg": "获取key成功",
  "data": "9d5ed678fe57bcca610140957afab571"
}

2.	获取实验信息接口
接口调用： /get_report

本接口实现	获取生信报告文件和信息
请求参数：
字段名称	字段说明	类型	必填	备注
appid	公钥	string	Y	公钥
startTime	开始时间	string	Y	2024-07-01 08:00:00
endTime	结束时间	string	Y	2024-07-01 20:00:00
page	页码号	int	N	1
limit	每页查询个数	int	N	1000
sign	加密签名	string	Y	签名，按照Sign生成算法
Sign生成算法：
对公钥 &私钥  使用 md5() 函数对连接后的字符串进行 MD5 加密，
例如：
sign = md5(appid=cwbioprimecx&appsecret=6c58e7a28cabb971926663834cb0ac2a)


请求示例：
{
  "appid": "cwbioprimecx",
  "sign": "225853a283c52500a2935b01b6ba026d",
  "startTime": "2024-07-01 08:00:00",
  "endTime": "2024-07-01 20:00:00",
  "page": 1, //可选
  "limit": 1000, //可选
}

返回参数（如果返回数据，需要加密）
中文名	字段名称	字段类型	备注
状态	code	String	200为成功 非200为不成功
信息提示	msg	String	成功信息提示
实验数据	data	String	成功的数据, 错误为空

数据字段定义：
中文名	字段名称	字段类型	备注
生信文件路径	report_path	String	
生成时间	report_time	String	包含年月日，时分秒

返回数据示例(以B22406270200, B22406270201为例)：
{
  "code": 200,
  "msg": "成功获取数据成功2条",
  "data": [
    {
      "report_path": "https://www.cwbio.com/uploads/gwservice/primecx/20240601/B22406270200.xls",
      "report_time": "2024-07-01 08:00:00"
    },
    {
      "report_path": "https://www.cwbio.com/uploads/gwservice/primecx/20240601/B22406270201.xls",
      "report_time": "2024-07-01 08:00:00"
    }
  ]
}

错误返回json格式, data为空或者包含样本编号
{
    "code": "201",
    "msg": "appid或appsecret不合法",
    "data": []
}
{
    "code": "202",
    "msg": "查无数据",
    "data": []
}
3.	推送实验结果接口
接口调用： /push_report

本接口实现推送实验结果，结果文件名称，请使用实验编号命名
请求参数：
字段名称	字段说明	类型	必填	备注
appid	公钥	string	Y	公钥
sign	加密签名	string	Y	参考上述算法
data	数据字段	array	Y	数据字段
数据字段定义：
中文名	字段名称	字段类型	备注
检测条码序号	detect_no	String	
测序状态	status	String	seqcanel- 测序失败，seqconfirm- 测序成功, ，seqabnormal - 测序异常
报告存放路径	report_path	String	以后使用oss，就是共享盘的路径, 以zip为后缀, 路径规范： /OSS/板号/检测条码序号.zip, 不传或者没有默认为减号(-)
测序结果说明	report_reason	String	测序结果说明，文本，不超过256个字符,可选
扩展字段	ext	String	额外字段，支持多个: 现有支持： 
plasmid_length -片段长度， 
sample_length - 质粒总长度
规则： 如果发送了ext字段，那么默认更新，如果有值，但是不匹配字段，不更新,同时记录日志到ext，


请求示例：
{
  "appid": "123456789",
  "sign": "225853a283c52500a2935b01b6ba026d",
  "data": [
    {
      "detect_no": "B22406270200",
      "status": "seqcancel",
      "report_path": "/OSS/20240601/B22406270200.zip",
      "report_reason": "测试失败，有原因，有传文件示例",
      "ext": {
        "plasmid_length": "100",
        "sample_length": "200"
      }
    },
    {
      "detect_no": "B22406270200",
      "status": "seqcancel",
      "report_path": "报告路径, 如果为空，传英文减号(-)",
      "report_reason": "测试失败, 没有报告说明， 传英文减号(-)"
    },
    {
      "detect_no": "B22406270201",
      "status": "seqconfirm",
      "report_path": "/OSS/20240601/B22406270201.zip",
      "report_reason": "成功可以不用传原因，可选"
    },
    {
      "detect_no": "B22406270201",
      "status": "seqabnormal",
      "report_path": "/OSS/20240601/B22406270201.zip",
      "report_reason": "测试异常, 没有报告说明， 传英文减号(-)"
    }
  ]
}

返回参数（如果返回数据，需要加密）
中文名	字段名称	字段类型	备注
状态	code	String	200为成功 非200为不成功
信息提示	msg	String	成功信息提示
实验数据	data	String	成功的数据, 成功为空


返回数据示例(以B22406270200, B22406270201为例)：
{
  "code": 200,
  "msg": "成功上传数据成功3条",
  "data": []
}

错误返回json格式, data为空或者包含样本编号
{
    "code": "201",
    "msg": "appid或appsecret不合法",
    "data": [xx – 显示失败的样本data]
}
{
    "code": "203",
    "msg": "上传成功1数据，失败1",
    "data": ["B22406270201"]
}
4.	错误代码参考
1.	返回错误参数
错误代码	错误信息提示
201	appid或appsecret不合法
202	查无数据
203	上传失败


