# coding=utf-8

import os
import time
import re
import io
import threading
import json
import random
import logging
from datetime import datetime

try:
    from httplib import BadStatusLine
except ImportError:
    from http.client import BadStatusLine

import requests
from pyqrcode import QRCode

from .. import conf, utils
from ..returnvalues import ReturnValue
from ..storage.templates import wrap_user_dict
from .contact import update_local_chatrooms, update_local_friends
from .messages import produce_msg

logger = logging.getLogger('itchat')


def load_login(core):
    core.login = login
    core.get_QRuuid = get_QRuuid
    core.get_QR = get_QR
    core.check_login = check_login
    core.web_init = web_init
    core.show_mobile_login = show_mobile_login
    core.start_receiving = start_receiving
    core.get_msg = get_msg
    core.logout = logout


def login(self, enableCmdQR=False, picDir=None, qrCallback=None,
          loginCallback=None, exitCallback=None):
    if self.alive or self.isLogging:
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + 'itchat已经运行，勿重复运行。')
        return
    self.isLogging = True
    while self.isLogging:
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '获取二维码的uuid')
        while not self.get_QRuuid():
            time.sleep(1)
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '下载二维码')
        qrStorage = self.get_QR(enableCmdQR=enableCmdQR,
                                picDir=picDir, qrCallback=qrCallback)
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '如果无法正常显示二维码，请手动打开程序目录下的QR.png图片')
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '请使用微信扫描二维码')
        isLoggedIn = False
        while not isLoggedIn:
            status = self.check_login()
            if hasattr(qrCallback, '__call__'):
                qrCallback(uuid=self.uuid, status=status,
                           qrcode=qrStorage.getvalue())
            if status == '200':
                isLoggedIn = True
            elif status == '201':
                if isLoggedIn is not None:
                    print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '请在10秒内确认登录')
                    isLoggedIn = None
                    time.sleep(10)
            elif status != '408':
                break
        if isLoggedIn:
            break
        elif self.isLogging:
            print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '登录超时，重新加载二维码')
    else:
        return  # log in process is stopped by user
    print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '加载联系人，这会消耗一些时间')
    self.web_init()
    self.show_mobile_login()
    self.get_contact(True)
    if hasattr(loginCallback, '__call__'):
        r = loginCallback()
    else:
        utils.clear_screen()
        if os.path.exists(picDir or conf.DEFAULT_QR):
            os.remove(picDir or conf.DEFAULT_QR)
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '你好，' + str(self.storageClass.nickName))
    self.start_receiving(exitCallback)
    self.isLogging = False


def push_login(core):
    cookiesDict = core.s.cookies.get_dict()
    if 'wxuin' in cookiesDict:
        url = '%s/cgi-bin/mmwebwx-bin/webwxpushloginurl?uin=%s' % (
            conf.BASE_URL, cookiesDict['wxuin'])
        headers = {'User-Agent': conf.USER_AGENT}
        r = core.s.get(url, headers=headers).json()
        if 'uuid' in r and r.get('ret') in (0, '0'):
            core.uuid = r['uuid']
            return r['uuid']
    return False


def get_QRuuid(self):
    url = '%s/jslogin' % conf.BASE_URL
    params = {
        'appid': 'wx782c26e4c19acffb',
        'fun': 'new',
        'redirect_uri': 'https://wx.qq.com/cgi-bin/mmwebwx-bin/webwxnewloginpage?mod=desktop',
        'lang': 'zh_CN'}
    headers = {'User-Agent': conf.USER_AGENT}
    r = self.s.get(url, params=params, headers=headers)
    regx = r'window.QRLogin.code = (\d+); window.QRLogin.uuid = "(\S+?)";'
    data = re.search(regx, r.text)
    if data and data.group(1) == '200':
        self.uuid = data.group(2)
        return self.uuid


def get_QR(self, uuid=None, enableCmdQR=False, picDir=None, qrCallback=None):
    uuid = uuid or self.uuid
    picDir = picDir or conf.DEFAULT_QR
    qrStorage = io.BytesIO()
    qrCode = QRCode('https://login.weixin.qq.com/l/' + uuid)
    qrCode.png(qrStorage, scale=10)
    if hasattr(qrCallback, '__call__'):
        qrCallback(uuid=uuid, status='0', qrcode=qrStorage.getvalue())
    else:
        with open(picDir, 'wb') as f:
            f.write(qrStorage.getvalue())
        if enableCmdQR:
            utils.print_cmd_qr(qrCode.text(1), enableCmdQR=enableCmdQR)
        else:
            utils.print_qr(picDir)
    return qrStorage


def check_login(self, uuid=None):
    uuid = uuid or self.uuid
    url = '%s/cgi-bin/mmwebwx-bin/login' % conf.BASE_URL
    localTime = int(time.time())
    params = 'loginicon=true&uuid=%s&tip=1&r=%s&_=%s' % (
        uuid, int(-localTime / 1579), localTime)
    headers = {'User-Agent': conf.USER_AGENT}
    r = self.s.get(url, params=params, headers=headers)
    regx = r'window.code=(\d+)'
    data = re.search(regx, r.text)
    if data and data.group(1) == '200':
        if process_login_info(self, r.text):
            return '200'
        else:
            return '400'
    elif data:
        return data.group(1)
    else:
        return '400'


def process_login_info(core, loginContent):
    ''' when finish login (scanning qrcode)
     * syncUrl and fileUploadingUrl will be fetched
     * deviceid and msgid will be generated
     * skey, wxsid, wxuin, pass_ticket will be fetched
    '''
    regx = r'window.redirect_uri="(\S+)";'
    core.loginInfo['url'] = re.search(regx, loginContent).group(1)
    headers = {'User-Agent': conf.USER_AGENT,
               'client-version': conf.UOS_PATCH_CLIENT_VERSION,
               'extspam': conf.UOS_PATCH_EXTSPAM,
               'referer': 'https://wx.qq.com/?&lang=zh_CN&target=t'
               }
    r = core.s.get(core.loginInfo['url'],
                   headers=headers, allow_redirects=False)
    core.loginInfo['url'] = core.loginInfo['url'][:core.loginInfo['url'].rfind(
        '/')]
    for indexUrl, detailedUrl in (
            ("wx2.qq.com", ("file.wx2.qq.com", "webpush.wx2.qq.com")),
            ("wx8.qq.com", ("file.wx8.qq.com", "webpush.wx8.qq.com")),
            ("qq.com", ("file.wx.qq.com", "webpush.wx.qq.com")),
            ("web2.wechat.com", ("file.web2.wechat.com", "webpush.web2.wechat.com")),
            ("wechat.com", ("file.web.wechat.com", "webpush.web.wechat.com"))):
        fileUrl, syncUrl = ['https://%s/cgi-bin/mmwebwx-bin' %
                            url for url in detailedUrl]
        if indexUrl in core.loginInfo['url']:
            core.loginInfo['fileUrl'], core.loginInfo['syncUrl'] = \
                fileUrl, syncUrl
            break
    else:
        core.loginInfo['fileUrl'] = core.loginInfo['syncUrl'] = core.loginInfo['url']
    core.loginInfo['deviceid'] = 'e' + repr(random.random())[2:17]
    core.loginInfo['logintime'] = int(time.time() * 1e3)
    core.loginInfo['BaseRequest'] = {}
    cookies = core.s.cookies.get_dict()
    skey = re.findall('<skey>(.*?)</skey>', r.text, re.S)[0]
    pass_ticket = re.findall('<pass_ticket>(.*?)</pass_ticket>', r.text, re.S)[0]
    core.loginInfo['skey'] = core.loginInfo['BaseRequest']['Skey'] = skey
    core.loginInfo['wxsid'] = core.loginInfo['BaseRequest']['Sid'] = cookies["wxsid"]
    core.loginInfo['wxuin'] = core.loginInfo['BaseRequest']['Uin'] = cookies["wxuin"]
    core.loginInfo['pass_ticket'] = pass_ticket
    # A question : why pass_ticket == DeviceID ?
    #               deviceID is only a randomly generated number

    # UOS PATCH By luvletter2333, Sun Feb 28 10:00 PM
    # for node in xml.dom.minidom.parseString(r.text).documentElement.childNodes:
    #     if node.nodeName == 'skey':
    #         core.loginInfo['skey'] = core.loginInfo['BaseRequest']['Skey'] = node.childNodes[0].data
    #     elif node.nodeName == 'wxsid':
    #         core.loginInfo['wxsid'] = core.loginInfo['BaseRequest']['Sid'] = node.childNodes[0].data
    #     elif node.nodeName == 'wxuin':
    #         core.loginInfo['wxuin'] = core.loginInfo['BaseRequest']['Uin'] = node.childNodes[0].data
    #     elif node.nodeName == 'pass_ticket':
    #         core.loginInfo['pass_ticket'] = core.loginInfo['BaseRequest']['DeviceID'] = node.childNodes[0].data
    if not all([key in core.loginInfo for key in ('skey', 'wxsid', 'wxuin', 'pass_ticket')]):
        print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '你的微信账号可能被限制登录网页版微信，错误信息：' + str(r.text))
        core.isLogging = False
        return False
    return True


def web_init(self):
    url = '%s/webwxinit' % self.loginInfo['url']
    params = {
        'r': int(-time.time() / 1579),
        'pass_ticket': self.loginInfo['pass_ticket'], }
    data = {'BaseRequest': self.loginInfo['BaseRequest'], }
    headers = {
        'ContentType': 'application/json; charset=UTF-8',
        'User-Agent': conf.USER_AGENT, }
    r = self.s.post(url, params=params, data=json.dumps(data), headers=headers)
    dic = json.loads(r.content.decode('utf-8', 'replace'))
    # deal with login info
    utils.emoji_formatter(dic['User'], 'NickName')
    self.loginInfo['InviteStartCount'] = int(dic['InviteStartCount'])
    self.loginInfo['User'] = wrap_user_dict(
        utils.struct_friend_info(dic['User']))
    self.memberList.append(self.loginInfo['User'])
    self.loginInfo['SyncKey'] = dic['SyncKey']
    self.loginInfo['synckey'] = '|'.join(['%s_%s' % (item['Key'], item['Val'])
                                          for item in dic['SyncKey']['List']])
    self.storageClass.userName = dic['User']['UserName']
    self.storageClass.nickName = dic['User']['NickName']
    # deal with contact list returned when init
    contactList = dic.get('ContactList', [])
    chatroomList, otherList = [], []
    for m in contactList:
        if m['Sex'] != 0:
            otherList.append(m)
        elif '@@' in m['UserName']:
            m['MemberList'] = []  # don't let dirty info pollute the list
            chatroomList.append(m)
        elif '@' in m['UserName']:
            # mp will be dealt in update_local_friends as well
            otherList.append(m)
    if chatroomList:
        update_local_chatrooms(self, chatroomList)
    if otherList:
        update_local_friends(self, otherList)
    return dic


def show_mobile_login(self):
    url = '%s/webwxstatusnotify?lang=zh_CN&pass_ticket=%s' % (
        self.loginInfo['url'], self.loginInfo['pass_ticket'])
    data = {
        'BaseRequest': self.loginInfo['BaseRequest'],
        'Code': 3,
        'FromUserName': self.storageClass.userName,
        'ToUserName': self.storageClass.userName,
        'ClientMsgId': int(time.time()), }
    headers = {
        'ContentType': 'application/json; charset=UTF-8',
        'User-Agent': conf.USER_AGENT, }
    r = self.s.post(url, data=json.dumps(data), headers=headers)
    return ReturnValue(rawResponse=r)


def start_receiving(self, exitCallback=None, getReceivingFnOnly=False):
    self.alive = True

    def maintain_loop():
        retryCount = 0
        while self.alive:
            try:
                i = sync_check(self)
                if i is None:
                    self.alive = False
                elif i == '0':
                    pass
                else:
                    msgList, contactList = self.get_msg()
                    if msgList:
                        msgList = produce_msg(self, msgList)
                        for msg in msgList:
                            self.msgList.put(msg)
                    if contactList:
                        chatroomList, otherList = [], []
                        for contact in contactList:
                            if '@@' in contact['UserName']:
                                chatroomList.append(contact)
                            else:
                                otherList.append(contact)
                        chatroomMsg = update_local_chatrooms(
                            self, chatroomList)
                        chatroomMsg['User'] = self.loginInfo['User']
                        self.msgList.put(chatroomMsg)
                        update_local_friends(self, otherList)
                retryCount = 0
            except requests.exceptions.ReadTimeout:
                pass
            except:
                retryCount += 1
                if self.receivingRetryCount < retryCount:
                    print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '基本信息获取失败且已达到最大重试次数，程序强制停止运行')
                    self.alive = False
                else:
                    time.sleep(1)
        self.logout()
        if hasattr(exitCallback, '__call__'):
            exitCallback()
    if getReceivingFnOnly:
        return maintain_loop
    else:
        maintainThread = threading.Thread(target=maintain_loop)
        maintainThread.setDaemon(True)
        maintainThread.start()


def sync_check(self):
    url = '%s/synccheck' % self.loginInfo.get('syncUrl', self.loginInfo['url'])
    params = {
        'r': int(time.time() * 1000),
        'skey': self.loginInfo['skey'],
        'sid': self.loginInfo['wxsid'],
        'uin': self.loginInfo['wxuin'],
        'deviceid': self.loginInfo['deviceid'],
        'synckey': self.loginInfo['synckey'],
        '_': self.loginInfo['logintime'], }
    headers = {'User-Agent': conf.USER_AGENT}
    self.loginInfo['logintime'] += 1
    try:
        r = self.s.get(url, params=params, headers=headers,
                       timeout=conf.TIMEOUT)
    except requests.exceptions.ConnectionError as e:
        try:
            if not isinstance(e.args[0].args[1], BadStatusLine):
                raise
            # will return a package with status '0 -'
            # and value like:
            # 6f:00:8a:9c:09:74:e4:d8:e0:14:bf:96:3a:56:a0:64:1b:a4:25:5d:12:f4:31:a5:30:f1:c6:48:5f:c3:75:6a:99:93
            # seems like status of typing, but before I make further achievement code will remain like this
            return '2'
        except:
            raise
    r.raise_for_status()
    regx = r'window.synccheck={retcode:"(\d+)",selector:"(\d+)"}'
    pm = re.search(regx, r.text)
    if pm is None or pm.group(1) != '0':
        if str(r.text) != 'window.synccheck={retcode:"1101",selector:"0"}':
            print(str(datetime.now().strftime('[%Y.%m.%d %H:%M:%S] ')) + '同步出错，错误信息：' + str(r.text))
        return None
    return pm.group(2)


def get_msg(self):
    self.loginInfo['deviceid'] = 'e' + repr(random.random())[2:17]
    url = '%s/webwxsync?sid=%s&skey=%s&pass_ticket=%s' % (
        self.loginInfo['url'], self.loginInfo['wxsid'],
        self.loginInfo['skey'], self.loginInfo['pass_ticket'])
    data = {
        'BaseRequest': self.loginInfo['BaseRequest'],
        'SyncKey': self.loginInfo['SyncKey'],
        'rr': ~int(time.time()), }
    headers = {
        'ContentType': 'application/json; charset=UTF-8',
        'User-Agent': conf.USER_AGENT}
    r = self.s.post(url, data=json.dumps(data),
                    headers=headers, timeout=conf.TIMEOUT)
    dic = json.loads(r.content.decode('utf-8', 'replace'))
    if str(dic.get('BaseResponse').get('Ret')) != '0':
        return None, None
    self.loginInfo['SyncKey'] = dic['SyncKey']
    self.loginInfo['synckey'] = '|'.join(['%s_%s' % (item['Key'], item['Val'])
                                          for item in dic['SyncCheckKey']['List']])
    return dic['AddMsgList'], dic['ModContactList']


def logout(self):
    if self.alive:
        url = '%s/webwxlogout' % self.loginInfo['url']
        params = {
            'redirect': 1,
            'type': 1,
            'skey': self.loginInfo['skey'], }
        headers = {'User-Agent': conf.USER_AGENT}
        self.s.get(url, params=params, headers=headers)
        self.alive = False
    self.isLogging = False
    self.s.cookies.clear()
    del self.chatroomList[:]
    del self.memberList[:]
    del self.mpList[:]
    return ReturnValue({'BaseResponse': {
        'ErrMsg': 'logout successfully.',
        'Ret': 0, }})
