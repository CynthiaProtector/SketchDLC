# Using Baidu Warp-CTC with MXNet


Baidu-WarpCTC is a CTC implementation by Baidu that supports using GPU processors. It supports using CTC with LSTM to solve label alignment problems in many areas, such as OCR and speech recognition.

You can get the source code for the example on [GitHub](https://github.com/dmlc/mxnet/tree/master/example/warpctc).

## Install Baidu Warp-CTC

```
  cd ~/
  git clone https://github.com/baidu-research/warp-ctc
  cd warp-ctc
  mkdir build
  cd build
  cmake ..
  make
  sudo make install
```

## Enable Warp-CTC in MXNet

```
  comment out following lines in make/config.mk
  WARPCTC_PATH = $(HOME)/warp-ctc
  MXNET_PLUGINS += plugin/warpctc/warpctc.mk

  rebuild mxnet by
  make clean && make -j4
```

## Run Examples

There are two examples. One is a toy example that validates CTC integration. The second is an OCR example with LSTM and CTC. You can run it by typing the following code:

```
  cd examples/warpctc
  python lstm_ocr.py
```

The OCR example is constructed as follows:

1. It generates a 80x30-pixel image for a 4-digit captcha using a Python captcha library.
2. The 80x30 image is used as 80 input for LSTM, and every input is one column of the image (a 30 dim vector).
3. The output layer use CTC loss.

The following code shows the detailed construction of the net: 

```
  def lstm_unroll(num_lstm_layer, seq_len,
                  num_hidden, num_label):
    param_cells = []
    last_states = []
    for i in range(num_lstm_layer):
        param_cells.append(LSTMParam(i2h_weight=mx.sym.Variable("l%d_i2h_weight" % i),
                                     i2h_bias=mx.sym.Variable("l%d_i2h_bias" % i),
                                     h2h_weight=mx.sym.Variable("l%d_h2h_weight" % i),
                                     h2h_bias=mx.sym.Variable("l%d_h2h_bias" % i)))
        state = LSTMState(c=mx.sym.Variable("l%d_init_c" % i),
                          h=mx.sym.Variable("l%d_init_h" % i))
        last_states.append(state)
    assert(len(last_states) == num_lstm_layer)
    data = mx.sym.Variable('data')
    label = mx.sym.Variable('label')

    #every column of image is an input, there are seq_len inputs
    wordvec = mx.sym.SliceChannel(data=data, num_outputs=seq_len, squeeze_axis=1)
    hidden_all = []
    for seqidx in range(seq_len):
        hidden = wordvec[seqidx]
        for i in range(num_lstm_layer):
            next_state = lstm(num_hidden, indata=hidden,
                              prev_state=last_states[i],
                              param=param_cells[i],
                              seqidx=seqidx, layeridx=i)
            hidden = next_state.h
            last_states[i] = next_state
        hidden_all.append(hidden)
    hidden_concat = mx.sym.Concat(*hidden_all, dim=0)
    pred = mx.sym.FullyConnected(data=hidden_concat, num_hidden=11)

    # here we do NOT need to transpose label as other lstm examples do
    label = mx.sym.Reshape(data=label, target_shape=(0,))
    #label should be int type, so use cast
    label = mx.sym.Cast(data = label, dtype = 'int32')
    sm = mx.sym.WarpCTC(data=pred, label=label, label_length = num_label, input_length = seq_len)
    return sm
```

## Supporting Multi-label Length

Provide labels with length b. For samples whose label length is smaller than b, append 0 to the label data to make it have length b.

0 is reserved for a blank label.

## Next Steps
* [MXNet tutorials index](http://mxnet.io/tutorials/index.html)
