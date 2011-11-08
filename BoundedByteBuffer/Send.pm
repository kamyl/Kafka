package Kafka::BoundedByteBuffer::Send;

use strict;
use Carp;
use File::Temp;
use Fcntl;

sub new {
    my $class = shift;
    my $freq = shift;

    my $self = {
        size    => $freq->size_in_bytes() + 2,
        buffer  => File::Temp->new(),
    };

    syswrite($self->{'buffer'}, pack('n', $freq->{'id'}));
    $freq->write_to( stream => $self->{'buffer'} );
    $self->{'buffer'}->seek(0, SEEK_SET);
    return bless $self;
}

sub write_request_size {
    my $self = shift;
    my $stream = shift;

    if ( ! $self->{'size_written'} ) {
        if ( ! syswrite($stream, pack('N', $self->{'size'})) ) {
            croak "Cannot write to stream";
        }
        $self->{'size_written'} = 1;
        return 4;
    }
    return 0;
}

sub write_to {
    my $self = shift;
    my $stream = shift;

    my $written = $self->write_request_size($stream);
    if ( $self->{'size_written'} && ! eof($self->{'buffer'}) ) {
        my $tmp;
        if ( ! sysread($self->{'buffer'}, $tmp, 8192) ) {
            croak "Could not read from stream";
        }
        $written += syswrite($stream, $tmp);
    }
    if ( eof($self->{'buffer'}) ) {
        $self->{'complete'} = 1;
        close($self->{'buffer'});
    }
    return $written;
}

sub write_completely {
    my $self = shift;
    my $stream = shift;

    my $written = 0;
    while ( ! $self->{'complete'} ) {
        $written += $self->write_to($stream);
    }
    return $written;
}

1;
