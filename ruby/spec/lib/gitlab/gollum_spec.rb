require 'spec_helper'

describe Gollum do
  describe Gollum::BlobEntry do
    describe '.normalize_dir' do
      it 'returns the path with normalized slashes' do
        expect(described_class.normalize_dir('foo/bar')).to eq('/foo/bar')
        expect(described_class.normalize_dir('/foo/bar')).to eq('/foo/bar')
        expect(described_class.normalize_dir('/foo/bar/')).to eq('/foo/bar')
        expect(described_class.normalize_dir('//foo//bar//')).to eq('/foo/bar')
      end

      it 'returns an empty string for toplevel paths' do
        expect(described_class.normalize_dir(nil)).to eq('')
        expect(described_class.normalize_dir('')).to eq('')
        expect(described_class.normalize_dir('.')).to eq('')
        expect(described_class.normalize_dir('..')).to eq('')
        expect(described_class.normalize_dir('/')).to eq('')
        expect(described_class.normalize_dir('//')).to eq('')
        expect(described_class.normalize_dir(' ')).to eq('/ ')
        expect(described_class.normalize_dir("\t")).to eq("/\t")
      end

      it 'does not expand tilde characters' do
        expect(described_class.normalize_dir('~/foo')).to eq('/~/foo')
        expect(described_class.normalize_dir('~root/foo')).to eq('/~root/foo')
        expect(described_class.normalize_dir('~!/foo')).to eq('/~!/foo')
        expect(described_class.normalize_dir('foo/~')).to eq('/foo/~')
      end
    end
  end
end
