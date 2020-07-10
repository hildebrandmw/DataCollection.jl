using DataCollection
using Test

# For checking behavior
using DataFrames
import DataStructures: SortedDict

struct ParametersA <: AbstractParameters
    a::Int
    b::Int
end

struct ParametersB <: AbstractParameters
    c::Int
    d::Int
end

struct ParametersC <: AbstractParameters
    e::Int
    f::Int
end

struct ParametersError <: AbstractParameters
    e::Int
    # Repeat `a` so we can test errors during merging
    a::Int
end

struct DataA <: AbstractData
    data::Int
end

@testset "Testing Parameters" begin
    x = ParametersA(1,2)
    @test DataCollection.dict(x) == SortedDict(:a => 1, :b => 2)

    y = ParametersB(3,4)
    expected = SortedDict(
        :a => 1,
        :b => 2,
        :c => 3,
        :d => 4,
    )
    @test DataCollection.dict(x,y) == expected

    z = ParametersError(5,6)
    @test_throws ErrorException DataCollection.dict(x,y,z)

    # DataFrames schenanigans
    df = DataFrame()

    x = ParametersA(1, 2)
    y = ParametersB(3, 4)
    _y = ParametersB(4, 4)
    d = DataA(10)

    # Create a row for these parameters and data
    addrow!(df, d, x, y)
    @test nrow(df) == 1
    @test DataCollection.hasrow(df, x, y)

    # Ensure that we don't get false positives
    @test !DataCollection.hasrow(df, x, _y)

    # Get this row and make sure the data propogated properly.
    row = df[1, :]
    @test row.data == d.data

    # Try updating.
    @test_throws ErrorException addrow!(df, d, x, y)
    d2 = DataA(20)
    addrow!(df, d2, x, y; force = true)
    row = df[1, :]
    @test row.data == 20

    # Now, add another row
    x2 = ParametersA(10, 20)
    y2 = ParametersB(30, 40)
    addrow!(df, d2, x2, y2)
    @test nrow(df) == 2
    @test DataCollection.hasrow(df, x2, y2)
    @test DataCollection.hasrow(df, x, y)

    # Add something with different parameters.
    # Make sure we get an error if we don't pass `cols = :union`
    z = ParametersC(1,2)
    @test_throws KeyError addrow!(df, d, z)
    addrow!(df, d, z; cols = :union)

    @test nrow(df) == 3

    # We should still have the rows we added earlier
    @test DataCollection.hasrow(df, x2, y2)
    @test DataCollection.hasrow(df, x, y)
    @test DataCollection.hasrow(df, z)
end

#####
##### MeasurementBundle
#####

struct TestMeasurement
    name::Symbol
    values
end

function Base.iterate(t::TestMeasurement, s = iterate(t.values))
    isnothing(s) && return nothing

    res = NamedTuple{(t.name,)}((s[1],))
    return res, iterate(t.values, s[2])
end

@testset "Testing MeasurementBundle" begin
    tm1 = TestMeasurement(:test_1, [1,2,3])
    tm2 = TestMeasurement(:test_2, [:a,:b])

    bundle = DataCollection.MeasurementBundle(
        (tm1, tm2),
        (pre = "hello",),
        (post = "bye",)
    )

    # First iteration
    s = iterate(bundle)
    @test !isnothing(s)
    @test s[1] == (pre = "hello", test_1 = 1, test_2 = :a, post = "bye")

    # Next iteration - should step through tm1 and tm2
    s = iterate(bundle, s[2])
    @test !isnothing(s)
    @test s[1] == (pre = "hello", test_1 = 2, test_2 = :b, post = "bye")

    # Now, tm2 should ge be dropped
    s = iterate(bundle, s[2])
    @test !isnothing(s)
    @test s[1] == (pre = "hello", test_1 = 3, post = "bye")

    # Final iteration, should just get nothing now
    s = iterate(bundle, s[2])
    @test isnothing(s)
end
