package Kafka::Message;

use strict;
use Kafka::Encode;
use Digest::CRC qw(crc32);

sub new {
    my $class = shift;
    my %args = @_;
    my $self = {
        payload     => substr($args{'data'}, 6),
        compression => substr($args{'data'}, 1, 1),
    };
    $self->{'crc'} = crc32($self->{'payload'});
    $self->{'size'} = length($self->{'payload'});
}

sub encode {
    my $self = shift;
    return Kafka::Encode::encode_message(message => $self->{'payload'});
}

sub is_valid {
    my $self = shift;
    return $self->{'crc'} == crc32($self->{'payload'});
}

sub to_string {
    my $self = shift;
    return 'message(magic = ' . $Kafka::Encode::CURRENT_MAGIC_VALUE . ', compression = ' . $self->{'compression'} .
                  ', crc = ' . $self->{'crc'} . ', payload = ' . $self->{'payload'} . ')';
}

1;
