use nalgebra::DMatrix;

use crate::error::ComputeError;
use crate::protocol::Frame;

#[derive(Debug)]
pub struct MatrixResult {
    pub rows: u8,
    pub cols: u8,
    pub data: Vec<f32>,
}

pub fn process_frame(frame: &Frame) -> Result<MatrixResult, ComputeError> {
    let Frame::Request { dims_a, dims_b, data } = frame else {
        return Err(ComputeError::InvalidFrame);
    };

    if dims_a.cols != dims_b.rows {
        return Err(ComputeError::DimensionMismatch);
    }

    let a_count = dims_a.rows as usize * dims_a.cols as usize;
    let b_count = dims_b.rows as usize * dims_b.cols as usize;

    if data.len() != a_count + b_count {
        return Err(ComputeError::InvalidFrame);
    }

    let a_data = &data[..a_count];
    let b_data = &data[a_count..];

    let a = DMatrix::from_row_slice(dims_a.rows as usize, dims_a.cols as usize, a_data);
    let b = DMatrix::from_row_slice(dims_b.rows as usize, dims_b.cols as usize, b_data);

    let result = &a * &b;

    let rows = result.nrows();
    let cols = result.ncols();
    let mut data = Vec::with_capacity(rows * cols);
    for i in 0..rows {
        for j in 0..cols {
            data.push(result[(i, j)]);
        }
    }

    Ok(MatrixResult {
        rows: dims_a.rows,
        cols: dims_b.cols,
        data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::MatrixDims;

    #[test]
    fn test_identity_multiplication() {
        let frame = Frame::Request {
            dims_a: MatrixDims { rows: 2, cols: 2 },
            dims_b: MatrixDims { rows: 2, cols: 2 },
            data: vec![1.0, 0.0, 0.0, 1.0, 5.0, 6.0, 7.0, 8.0],
        };
        let result = process_frame(&frame).unwrap();
        assert_eq!(result.rows, 2);
        assert_eq!(result.cols, 2);
        assert!((result.data[0] - 5.0).abs() < 1e-5);
        assert!((result.data[1] - 6.0).abs() < 1e-5);
        assert!((result.data[2] - 7.0).abs() < 1e-5);
        assert!((result.data[3] - 8.0).abs() < 1e-5);
    }

    #[test]
    fn test_2x3_times_3x2() {
        // A = [[1,2,3],[4,5,6]], B = [[7,8],[9,10],[11,12]]
        // A*B = [[58,64],[139,154]]
        let frame = Frame::Request {
            dims_a: MatrixDims { rows: 2, cols: 3 },
            dims_b: MatrixDims { rows: 3, cols: 2 },
            data: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        };
        let result = process_frame(&frame).unwrap();
        assert_eq!(result.rows, 2);
        assert_eq!(result.cols, 2);
        assert!((result.data[0] - 58.0).abs() < 1e-4);
        assert!((result.data[1] - 64.0).abs() < 1e-4);
        assert!((result.data[2] - 139.0).abs() < 1e-4);
        assert!((result.data[3] - 154.0).abs() < 1e-4);
    }

    #[test]
    fn test_dimension_mismatch() {
        let frame = Frame::Request {
            dims_a: MatrixDims { rows: 2, cols: 3 },
            dims_b: MatrixDims { rows: 2, cols: 2 },
            data: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        };
        match process_frame(&frame) {
            Err(ComputeError::DimensionMismatch) => {}
            other => panic!("Expected DimensionMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_1x1_scalar() {
        let frame = Frame::Request {
            dims_a: MatrixDims { rows: 1, cols: 1 },
            dims_b: MatrixDims { rows: 1, cols: 1 },
            data: vec![3.0, 4.0],
        };
        let result = process_frame(&frame).unwrap();
        assert_eq!(result.data.len(), 1);
        assert!((result.data[0] - 12.0).abs() < 1e-5);
    }
}
