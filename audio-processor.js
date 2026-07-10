/**
 * AudioWorklet Processor for Tooth Dental Care AI Receptionist
 * Captures mic input, downsamples to 16kHz mono PCM16, posts to main thread.
 */
class AudioCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this._bufferSize = 2048;
    this._buffer = new Float32Array(this._bufferSize);
    this._bytesWritten = 0;
  }

  /**
   * Downsample float32 audio from native sample rate to 16kHz
   */
  _downsample(input, inputSampleRate, outputSampleRate) {
    if (inputSampleRate === outputSampleRate) {
      return input;
    }
    const ratio = inputSampleRate / outputSampleRate;
    const outputLength = Math.round(input.length / ratio);
    const output = new Float32Array(outputLength);
    for (let i = 0; i < outputLength; i++) {
      const srcIndex = i * ratio;
      const srcIndexFloor = Math.floor(srcIndex);
      const srcIndexCeil = Math.min(srcIndexFloor + 1, input.length - 1);
      const frac = srcIndex - srcIndexFloor;
      output[i] = input[srcIndexFloor] * (1 - frac) + input[srcIndexCeil] * frac;
    }
    return output;
  }

  /**
   * Convert Float32 [-1, 1] to Int16 PCM
   */
  _floatTo16BitPCM(float32Array) {
    const pcm16 = new Int16Array(float32Array.length);
    for (let i = 0; i < float32Array.length; i++) {
      let s = Math.max(-1, Math.min(1, float32Array[i]));
      pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
    }
    return pcm16;
  }

  /**
   * Flush accumulated buffer
   */
  _flush() {
    const downsampled = this._downsample(
      this._buffer.slice(0, this._bytesWritten),
      sampleRate, // global in AudioWorklet scope
      16000
    );
    const pcm16 = this._floatTo16BitPCM(downsampled);
    this.port.postMessage(pcm16.buffer, [pcm16.buffer]);
    this._bytesWritten = 0;
  }

  process(inputs, outputs, parameters) {
    const input = inputs[0];
    if (!input || !input.length || !input[0]) {
      return true;
    }

    const channelData = input[0]; // mono channel

    for (let i = 0; i < channelData.length; i++) {
      this._buffer[this._bytesWritten++] = channelData[i];
      if (this._bytesWritten >= this._bufferSize) {
        this._flush();
      }
    }

    return true;
  }
}

registerProcessor('audio-capture-processor', AudioCaptureProcessor);
