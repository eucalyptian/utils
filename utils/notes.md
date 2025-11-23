# environment: cmd or git
command: `git push -u origin main`  
error: fatal: unable to access 'https://github.com/eucalyptian/ime-options.git/': SSL peer certificate or SSH remote key was not OK  
solution: `git config --global http.sslVerify false`  
note: re-enable ssl quickly after pushing repo to remote: `git config --global http.sslVerify true`  

---

