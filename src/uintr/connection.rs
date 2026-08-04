// Unix Domain Socket 文件描述符传递

use std::os::unix::io::{RawFd, AsRawFd};
use std::os::unix::net::UnixStream;
use super::{UintrError, UintrResult};

pub fn send_fd(socket: &UnixStream, fd: RawFd) -> UintrResult<()> {
    unsafe {
        use libc::{msghdr, iovec, sendmsg, CMSG_FIRSTHDR, CMSG_DATA, SOL_SOCKET, SCM_RIGHTS};

        let mut buf = [0u8; 1];
        let mut iov = iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: 1,
        };

        let fd_size = std::mem::size_of::<RawFd>();
        let cmsg_space_size = libc::CMSG_SPACE(fd_size as u32) as usize;
        let mut cmsg_space: Vec<u8> = vec![0; cmsg_space_size];

        let msg = msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut iov,
            msg_iovlen: 1,
            msg_control: cmsg_space.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_space.len(),
            msg_flags: 0,
        };

        let cmsg = CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = SOL_SOCKET;
        (*cmsg).cmsg_type = SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(fd_size as u32) as usize;

        let data = CMSG_DATA(cmsg);
        *(data as *mut RawFd) = fd;

        let result = sendmsg(socket.as_raw_fd(), &msg, 0);
        if result < 0 {
            return Err(UintrError::SendFdError(format!(
                "send_fd failed: {}",
                std::io::Error::last_os_error()
            )));
        }
    }
    Ok(())
}

pub fn recv_fd(socket: &UnixStream) -> UintrResult<RawFd> {
    unsafe {
        use libc::{msghdr, iovec, recvmsg, CMSG_FIRSTHDR, CMSG_DATA, SOL_SOCKET, SCM_RIGHTS};

        let mut buf = [0u8; 1];
        let mut iov = iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: 1,
        };

        let fd_size = std::mem::size_of::<RawFd>();
        let cmsg_space_size = libc::CMSG_SPACE(fd_size as u32) as usize;
        let mut cmsg_space: Vec<u8> = vec![0; cmsg_space_size];

        let mut msg = msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &mut iov,
            msg_iovlen: 1,
            msg_control: cmsg_space.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_space.len(),
            msg_flags: 0,
        };

        let result = recvmsg(socket.as_raw_fd(), &mut msg, 0);
        if result < 0 {
            return Err(UintrError::RecvFdError(format!(
                "recv_fd failed: {}",
                std::io::Error::last_os_error()
            )));
        }

        let cmsg = CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() || (*cmsg).cmsg_level != SOL_SOCKET || (*cmsg).cmsg_type != SCM_RIGHTS {
            return Err(UintrError::RecvFdError(
                "recv_fd: no file descriptor received".to_string()
            ));
        }

        let data = CMSG_DATA(cmsg);
        let fd = *(data as *const RawFd);
        Ok(fd)
    }
}
