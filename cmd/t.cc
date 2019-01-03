#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>


int main() 
{
  int fd = open("testdb/LOCK", O_RDWR, 0644);
  if (fd < 0) 
  {
    perror("open failed:");
  }
  struct flock file_lock_info;
  file_lock_info.l_type = F_WRLCK;
  file_lock_info.l_whence = SEEK_SET;
  file_lock_info.l_start = 0;
  file_lock_info.l_len = 0;
  int ret = fcntl(fd, F_SETLK, &file_lock_info);
  if (ret!=0)
  {
    perror("lock file failed:");
  }
  printf("%d %d\n", ret, fd);
  while(1)
  {
    sleep(11);
  }
}

